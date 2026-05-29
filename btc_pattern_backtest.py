"""BTC pattern backtests for manual Telegram commands only.

No trading side effects: this module only reads OHLCV and returns statistics.
Supports 4H fingerprints + US-open sweep and 1H fingerprints.
"""
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd

from debug_log import log_event

TF_MS = 4 * 60 * 60 * 1000


def _tf_ms(timeframe: str) -> int:
    tf = str(timeframe or "4h").lower().strip()
    if tf in ("15m", "15min", "minute15", "min15"):
        return 15 * 60 * 1000
    if tf in ("1h", "1hour", "hour1"):
        return 60 * 60 * 1000
    return 4 * 60 * 60 * 1000


def _mexc_interval(timeframe: str) -> str:
    tf = str(timeframe or "4h").lower().strip()
    return "Min15" if tf in ("15m", "15min", "minute15", "min15") else ("Hour1" if tf in ("1h", "1hour", "hour1") else "Hour4")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default


def _fmt_pct(x: float) -> str:
    try:
        return f"{x*100:.2f}%"
    except Exception:
        return "n/a"


def _profit_factor(returns: list[float]) -> float:
    wins = sum(r for r in returns if r > 0)
    losses = -sum(r for r in returns if r < 0)
    if losses <= 1e-12:
        return 99.0 if wins > 0 else 0.0
    return wins / losses


def _max_drawdown(equity: list[float]) -> float:
    peak = 0.0
    dd = 0.0
    for x in equity:
        peak = max(peak, x)
        dd = min(dd, x - peak)
    return dd


async def fetch_ohlcv_history(exchange, symbol: str = "BTC_USDT", timeframe: str = "4h", years: float = 3.0, limit_per_call: int = 200) -> list[list[float]]:
    """Fetch up to N years of candles with ccxt pagination when possible.

    Falls back to the bot's normal fetch_ohlcv if pagination is unavailable.
    """
    now_ms = int(time.time() * 1000)
    tf_ms = _tf_ms(timeframe)
    since_ms = now_ms - int(float(years) * 365.25 * 24 * 60 * 60 * 1000)
    rows: list[list[float]] = []
    seen: set[int] = set()
    limit = max(50, min(int(limit_per_call or 200), 500))
    cur = since_ms
    norm_symbol = symbol
    try:
        norm_symbol = exchange.normalize_symbol(symbol)
    except Exception:
        norm_symbol = "BTC/USDT:USDT" if str(symbol).upper().replace("_", "").startswith("BTCUSDT") else symbol

    # Prefer ccxt because it supports `since` pagination on many venues.
    ccxt_ex = getattr(exchange, "exchange", None)
    if ccxt_ex is not None and hasattr(ccxt_ex, "fetch_ohlcv"):
        for _ in range(260):  # enough for ~3y of 15m candles with 500 limit; safety cap.
            try:
                batch = await asyncio.wait_for(ccxt_ex.fetch_ohlcv(norm_symbol, timeframe=timeframe, since=cur, limit=limit), timeout=15)
            except Exception as e:
                log_event("btc_pattern_backtest_fetch_ccxt_error", ok=False, error=str(e)[:400], since=cur)
                break
            if not batch:
                break
            advanced = False
            for r in batch:
                try:
                    ts = int(float(r[0])); ts = ts * 1000 if ts < 10_000_000_000 else ts
                    if ts < since_ms or ts in seen:
                        continue
                    rows.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] if len(r) > 5 else 0)])
                    seen.add(ts)
                    advanced = True
                except Exception:
                    continue
            last_ts = int(float(batch[-1][0])); last_ts = last_ts * 1000 if last_ts < 10_000_000_000 else last_ts
            nxt = last_ts + tf_ms
            if nxt <= cur:
                break
            cur = nxt
            if cur >= now_ms - TF_MS:
                break
            if not advanced and len(batch) < 2:
                break
            await asyncio.sleep(0.05)
    if len(rows) >= 500:
        rows = sorted(rows, key=lambda x: x[0])
        return rows

    # Native MEXC public fallback with start/end pagination.  This is still read-only.
    if hasattr(exchange, "_mexc_public"):
        try:
            msym = exchange.mexc_contract_symbol(symbol) if hasattr(exchange, "mexc_contract_symbol") else "BTC_USDT"
            cur_sec = since_ms // 1000
            end_all_sec = now_ms // 1000
            step_sec = int(tf_ms // 1000 * 190)
            while cur_sec < end_all_sec and len(rows) < 150000:
                end_sec = min(end_all_sec, cur_sec + step_sec)
                resp = await asyncio.wait_for(exchange._mexc_public("GET", f"/api/v1/contract/kline/{msym}", query={"interval": _mexc_interval(timeframe), "start": cur_sec, "end": end_sec}), timeout=15)
                data = resp.get("data") if isinstance(resp, dict) else resp
                if isinstance(data, dict) and all(k in data for k in ("time", "open", "close", "high", "low")):
                    vols = data.get("vol") or data.get("volume") or [0] * len(data.get("time") or [])
                    for t,o,c,h,l,v in zip(data.get("time") or [], data.get("open") or [], data.get("close") or [], data.get("high") or [], data.get("low") or [], vols):
                        ts = int(float(t)); ts = ts * 1000 if ts < 10_000_000_000 else ts
                        if ts >= since_ms and ts not in seen:
                            rows.append([ts, float(o), float(h), float(l), float(c), float(v or 0)])
                            seen.add(ts)
                cur_sec = end_sec + 1
                await asyncio.sleep(0.05)
            if len(rows) >= 500:
                rows = sorted(rows, key=lambda x: x[0])
                return rows
        except Exception as e:
            log_event("btc_pattern_backtest_fetch_native_error", ok=False, error=str(e)[:400])

    # Fallback: current bot method; enough for a smoke test but not 3y.
    try:
        batch = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        for r in batch or []:
            ts = int(float(r[0])); ts = ts * 1000 if ts < 10_000_000_000 else ts
            if ts not in seen:
                rows.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] if len(r) > 5 else 0)])
                seen.add(ts)
    except Exception as e:
        log_event("btc_pattern_backtest_fetch_fallback_error", ok=False, error=str(e)[:400])
    return sorted(rows, key=lambda x: x[0])


def _to_df(candles: list[list[float]]) -> pd.DataFrame:
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["msk"] = df["dt"] + pd.Timedelta(hours=3)
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=3).mean().bfill().replace(0, np.nan)
    df["vol_ma30"] = df["volume"].rolling(30, min_periods=3).mean().bfill().replace(0, np.nan)
    df["ma7"] = df["close"].rolling(7, min_periods=3).mean()
    df["ma25"] = df["close"].rolling(25, min_periods=8).mean()
    df["ma99"] = df["close"].rolling(99, min_periods=20).mean()
    return df


def _window_features(df: pd.DataFrame, window: int) -> np.ndarray:
    o = df["open"].to_numpy(float)
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    v = df["volume"].to_numpy(float)
    atr = df["atr14"].to_numpy(float)
    vma = df["vol_ma30"].to_numpy(float)
    ma7 = df["ma7"].to_numpy(float)
    ma25 = df["ma25"].to_numpy(float)
    ma99 = df["ma99"].to_numpy(float)
    n = len(df)
    feats = []
    for i in range(window - 1, n):
        start = i - window + 1
        base_close = max(c[start], 1e-9)
        local_atr = np.nanmean(atr[max(0, i - 13): i + 1])
        if not np.isfinite(local_atr) or local_atr <= 0:
            local_atr = max(np.nanmean(h[start:i+1] - l[start:i+1]), base_close * 0.005, 1e-9)
        row: list[float] = []
        for k in range(start, i + 1):
            rng = max(h[k] - l[k], base_close * 1e-9)
            body = (c[k] - o[k]) / local_atr
            upper = (h[k] - max(o[k], c[k])) / local_atr
            lower = (min(o[k], c[k]) - l[k]) / local_atr
            close_pos = (c[k] - l[k]) / rng
            candle_range = rng / local_atr
            vol_ratio = v[k] / max(vma[k] if np.isfinite(vma[k]) else np.nanmean(v[max(0, k-29):k+1]), 1e-9)
            row.extend([body, upper, lower, close_pos - 0.5, candle_range, math.log(max(vol_ratio, 1e-6))])
        # Context features at the end of the pattern.
        row.extend([
            (c[i] - c[start]) / max(local_atr, 1e-9),
            (c[i] - (ma7[i] if np.isfinite(ma7[i]) else c[i])) / max(local_atr, 1e-9),
            (c[i] - (ma25[i] if np.isfinite(ma25[i]) else c[i])) / max(local_atr, 1e-9),
            (c[i] - (ma99[i] if np.isfinite(ma99[i]) else c[i])) / max(local_atr, 1e-9),
            ((ma7[i] if np.isfinite(ma7[i]) else c[i]) - (ma25[i] if np.isfinite(ma25[i]) else c[i])) / max(local_atr, 1e-9),
        ])
        feats.append(row)
    X = np.asarray(feats, dtype=float)
    X = np.nan_to_num(X, nan=0.0, posinf=6.0, neginf=-6.0)
    # Robust clipping prevents one abnormal volume wick from dominating distance.
    return np.clip(X, -8.0, 8.0)


def pattern_match_backtest(df: pd.DataFrame, window: int = 6, horizon: int = 6, top_k: int = 30, min_train: int = 360, min_edge: float = 0.60) -> dict:
    if len(df) < min_train + window + horizon + 50:
        return {"ok": False, "error": f"not enough candles: {len(df)}"}
    X = _window_features(df, window)
    closes = df["close"].to_numpy(float)
    # Feature row r corresponds to candle index i = r + window - 1.
    returns: list[float] = []
    directions: list[str] = []
    sample_count = 0
    skipped = 0
    equity: list[float] = []
    eq = 0.0
    pred_records: list[dict] = []
    first_i = max(min_train, window + horizon + 10)
    last_i = len(df) - horizon - 1
    for i in range(first_i, last_i):
        r_idx = i - window + 1
        train_end_i = i - horizon  # no overlapping future leakage.
        train_rows = train_end_i - window + 1
        if train_rows < top_k + 20 or r_idx >= len(X):
            continue
        current = X[r_idx]
        hist = X[:train_rows]
        d = np.mean((hist - current) ** 2, axis=1)
        if len(d) < top_k:
            continue
        nn = np.argpartition(d, top_k)[:top_k]
        fut = []
        for rr in nn:
            j = int(rr + window - 1)
            if j + horizon >= len(closes):
                continue
            fut.append((closes[j + horizon] - closes[j]) / max(closes[j], 1e-9))
        if len(fut) < max(12, int(top_k * 0.65)):
            skipped += 1
            continue
        pos_rate = sum(1 for x in fut if x > 0) / len(fut)
        avg = float(np.mean(fut))
        if pos_rate >= min_edge and avg > 0:
            side = "LONG"
            ret = (closes[i + horizon] - closes[i]) / max(closes[i], 1e-9)
        elif pos_rate <= (1.0 - min_edge) and avg < 0:
            side = "SHORT"
            ret = (closes[i] - closes[i + horizon]) / max(closes[i], 1e-9)
        else:
            skipped += 1
            continue
        # Rough trading cost: 0.06% round-trip placeholder; configurable later.
        ret_net = ret - 0.0006
        returns.append(float(ret_net))
        directions.append(side)
        eq += float(ret_net)
        equity.append(eq)
        sample_count += 1
        if len(pred_records) < 5 or i > last_i - 100:
            pred_records.append({"ts": int(df.iloc[i]["ts"]), "side": side, "pos_rate": round(pos_rate, 3), "avg_future": round(avg, 5), "ret_net": round(ret_net, 5)})
    if not returns:
        return {"ok": True, "window": window, "horizon": horizon, "top_k": top_k, "signals": 0, "skipped": skipped, "message": "no historical edge >= threshold"}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    return {
        "ok": True,
        "window": window,
        "horizon": horizon,
        "top_k": top_k,
        "signals": len(returns),
        "skipped": skipped,
        "winrate": len(wins) / len(returns),
        "profit_factor": _profit_factor(returns),
        "avg_return": float(np.mean(returns)),
        "median_return": float(np.median(returns)),
        "max_drawdown_sum_return": _max_drawdown(equity),
        "long_signals": directions.count("LONG"),
        "short_signals": directions.count("SHORT"),
        "examples": pred_records[-8:],
    }



def pattern_match_backtest_knn(df: pd.DataFrame, window: int = 24, horizon: int = 24, top_k: int = 40, min_train: int = 1200, min_edge: float = 0.60, max_neighbors: int = 500, eval_stride: int = 3) -> dict:
    """Fast approximate walk-forward pattern test for larger 1H datasets.

    Uses nearest-neighbor search over candle fingerprints, then for each decision keeps
    only neighbors that were strictly older than the decision candle. No trading side effects.
    This is much faster than full O(N^2) scanning and is intended for manual backtests.
    """
    if len(df) < min_train + window + horizon + 50:
        return {"ok": False, "error": f"not enough candles: {len(df)}"}
    X = _window_features(df, window)
    closes = df["close"].to_numpy(float)
    n = len(X)
    if n < top_k + 50:
        return {"ok": False, "error": f"not enough feature rows: {n}"}
    # Standardize to prevent one feature group from dominating.
    mu = np.nanmean(X, axis=0)
    sig = np.nanstd(X, axis=0)
    sig = np.where(sig < 1e-9, 1.0, sig)
    Xs = np.nan_to_num((X - mu) / sig, nan=0.0, posinf=6.0, neginf=-6.0)
    k_search = int(min(max_neighbors, max(top_k * 8, top_k + 50), max(2, n - 1)))
    try:
        from sklearn.neighbors import NearestNeighbors
        nn_model = NearestNeighbors(n_neighbors=k_search, algorithm="auto", metric="euclidean")
        nn_model.fit(Xs)
        distances, indices = nn_model.kneighbors(Xs, return_distance=True)
    except Exception as e:
        log_event("btc_pattern_backtest_knn_error", ok=False, error=str(e)[:500], window=window, horizon=horizon)
        return pattern_match_backtest(df, window=window, horizon=horizon, top_k=top_k, min_train=min_train, min_edge=min_edge)

    returns: list[float] = []
    directions: list[str] = []
    equity: list[float] = []
    eq = 0.0
    skipped = 0
    evaluated = 0
    pred_records: list[dict] = []
    first_i = max(min_train, window + horizon + 10)
    last_i = len(df) - horizon - 1
    stride = max(1, int(eval_stride or 1))
    for i in range(first_i, last_i, stride):
        r_idx = i - window + 1
        if r_idx < 0 or r_idx >= len(indices):
            continue
        evaluated += 1
        train_end_i = i - horizon
        train_rows = train_end_i - window + 1
        if train_rows < top_k + 20:
            continue
        fut = []
        picked = 0
        for rr in indices[r_idx]:
            rr = int(rr)
            if rr == r_idx or rr >= train_rows:
                continue
            j = rr + window - 1
            if j + horizon >= len(closes):
                continue
            fut.append((closes[j + horizon] - closes[j]) / max(closes[j], 1e-9))
            picked += 1
            if picked >= top_k:
                break
        if len(fut) < max(12, int(top_k * 0.65)):
            skipped += 1
            continue
        pos_rate = sum(1 for x in fut if x > 0) / len(fut)
        avg = float(np.mean(fut))
        if pos_rate >= min_edge and avg > 0:
            side = "LONG"
            ret = (closes[i + horizon] - closes[i]) / max(closes[i], 1e-9)
        elif pos_rate <= (1.0 - min_edge) and avg < 0:
            side = "SHORT"
            ret = (closes[i] - closes[i + horizon]) / max(closes[i], 1e-9)
        else:
            skipped += 1
            continue
        ret_net = ret - 0.0006
        returns.append(float(ret_net))
        directions.append(side)
        eq += float(ret_net)
        equity.append(eq)
        if len(pred_records) < 5 or i > last_i - 200:
            pred_records.append({"ts": int(df.iloc[i]["ts"]), "side": side, "pos_rate": round(pos_rate, 3), "avg_future": round(avg, 5), "ret_net": round(ret_net, 5)})
    if not returns:
        return {"ok": True, "window": window, "horizon": horizon, "top_k": top_k, "signals": 0, "skipped": skipped, "evaluated": evaluated, "message": "no historical edge >= threshold", "fast_knn": True}
    wins = [r for r in returns if r > 0]
    return {
        "ok": True,
        "window": window,
        "horizon": horizon,
        "top_k": top_k,
        "signals": len(returns),
        "skipped": skipped,
        "evaluated": evaluated,
        "eval_stride": stride,
        "fast_knn": True,
        "winrate": len(wins) / len(returns),
        "profit_factor": _profit_factor(returns),
        "avg_return": float(np.mean(returns)),
        "median_return": float(np.median(returns)),
        "max_drawdown_sum_return": _max_drawdown(equity),
        "long_signals": directions.count("LONG"),
        "short_signals": directions.count("SHORT"),
        "examples": pred_records[-8:],
    }


def current_pattern_stats(df: pd.DataFrame, window: int = 6, horizon: int = 6, top_k: int = 30) -> dict:
    if len(df) < window + horizon + top_k + 50:
        return {"ok": False, "error": "not enough candles"}
    X = _window_features(df, window)
    closes = df["close"].to_numpy(float)
    r_idx = len(X) - 1
    current = X[r_idx]
    # Exclude last horizon candles from history to avoid comparing with immediate overlap.
    hist_end = max(0, r_idx - horizon)
    hist = X[:hist_end]
    if len(hist) < top_k:
        return {"ok": False, "error": "not enough history"}
    d = np.mean((hist - current) ** 2, axis=1)
    nn = np.argpartition(d, top_k)[:top_k]
    fut = []
    cases = []
    for rr in nn:
        j = int(rr + window - 1)
        if j + horizon >= len(closes):
            continue
        ret = (closes[j + horizon] - closes[j]) / max(closes[j], 1e-9)
        fut.append(ret)
        cases.append({"ts": int(df.iloc[j]["ts"]), "future_ret": round(float(ret), 5), "dist": round(float(d[rr]), 5)})
    if not fut:
        return {"ok": False, "error": "no neighbors with future"}
    pos_rate = sum(1 for x in fut if x > 0) / len(fut)
    side = "WAIT"
    if pos_rate >= 0.60 and float(np.mean(fut)) > 0:
        side = "LONG"
    elif pos_rate <= 0.40 and float(np.mean(fut)) < 0:
        side = "SHORT"
    return {
        "ok": True,
        "window": window,
        "horizon": horizon,
        "cases": len(fut),
        "suggested": side,
        "up_rate": pos_rate,
        "avg_future_return": float(np.mean(fut)),
        "median_future_return": float(np.median(fut)),
        "best_case": float(np.max(fut)),
        "worst_case": float(np.min(fut)),
        "nearest_examples": cases[:8],
    }


def _simulate_trade_path(df: pd.DataFrame, start_idx: int, side: str, entry: float, sl: float, tp: float, max_bars: int) -> tuple[float, str, int]:
    # Return net return pct in decimal, outcome, bars held. Entry is assumed at start_idx close.
    for k in range(start_idx + 1, min(len(df), start_idx + 1 + max_bars)):
        hi = float(df.iloc[k]["high"]); lo = float(df.iloc[k]["low"])
        if side == "LONG":
            hit_sl = lo <= sl
            hit_tp = hi >= tp
            if hit_sl and hit_tp:
                # Conservative: SL first if both within same 4H candle.
                return (sl - entry) / entry - 0.0006, "SL_BOTH", k - start_idx
            if hit_sl:
                return (sl - entry) / entry - 0.0006, "SL", k - start_idx
            if hit_tp:
                return (tp - entry) / entry - 0.0006, "TP", k - start_idx
        else:
            hit_sl = hi >= sl
            hit_tp = lo <= tp
            if hit_sl and hit_tp:
                return (entry - sl) / entry - 0.0006, "SL_BOTH", k - start_idx
            if hit_sl:
                return (entry - sl) / entry - 0.0006, "SL", k - start_idx
            if hit_tp:
                return (entry - tp) / entry - 0.0006, "TP", k - start_idx
    exit_price = float(df.iloc[min(len(df)-1, start_idx + max_bars)]["close"])
    ret = (exit_price - entry) / entry if side == "LONG" else (entry - exit_price) / entry
    return ret - 0.0006, "TIME", max_bars


def us_open_4h_sweep_backtest(df: pd.DataFrame, break_pct: float = 0.0015, buffer_pct: float = 0.0005, max_bars: int = 6) -> dict:
    """Test 15:00-19:00 MSK 4H candle sweep and return.

    Reference candle: open hour 15 MSK, the 4H candle containing 16:30 MSK US open.
    After it closes, next candles are checked for sweep outside ref high/low and close back inside.
    Entry: close of return candle.
    SL: sweep extreme plus buffer.
    TP: midpoint of reference candle.
    """
    if len(df) < 200:
        return {"ok": False, "error": "not enough candles"}
    rets: list[float] = []
    trades = []
    equity = []
    eq = 0.0
    # map date -> ref candle index, open hour 15 MSK.
    ref_indices = [i for i, row in df.iterrows() if int(row["msk"].hour) == 15]
    for idx in ref_indices:
        if idx + 2 >= len(df):
            continue
        ref = df.iloc[idx]
        ref_high = float(ref["high"]); ref_low = float(ref["low"]); mid = (ref_high + ref_low) / 2.0
        until = min(len(df), idx + 1 + max_bars)
        entered = False
        for k in range(idx + 1, until):
            row = df.iloc[k]
            hi = float(row["high"]); lo = float(row["low"]); close = float(row["close"])
            # Sweep up and close back inside -> short.
            if hi >= ref_high * (1 + break_pct) and close < ref_high and close > ref_low:
                entry = close
                sl = max(hi, ref_high) * (1 + buffer_pct)
                tp = mid
                if tp >= entry or sl <= entry:
                    continue
                ret, outcome, bars = _simulate_trade_path(df, k, "SHORT", entry, sl, tp, max_bars=max_bars)
                rets.append(ret); eq += ret; equity.append(eq)
                trades.append({"ts": int(row["ts"]), "side": "SHORT", "ret": round(ret, 5), "outcome": outcome, "bars": bars})
                entered = True
                break
            # Sweep down and close back inside -> long.
            if lo <= ref_low * (1 - break_pct) and close > ref_low and close < ref_high:
                entry = close
                sl = min(lo, ref_low) * (1 - buffer_pct)
                tp = mid
                if tp <= entry or sl >= entry:
                    continue
                ret, outcome, bars = _simulate_trade_path(df, k, "LONG", entry, sl, tp, max_bars=max_bars)
                rets.append(ret); eq += ret; equity.append(eq)
                trades.append({"ts": int(row["ts"]), "side": "LONG", "ret": round(ret, 5), "outcome": outcome, "bars": bars})
                entered = True
                break
        if not entered:
            continue
    if not rets:
        return {"ok": True, "signals": 0, "message": "no sweep-return trades with current rules"}
    wins = [r for r in rets if r > 0]
    return {
        "ok": True,
        "reference": "15:00-19:00 MSK 4H candle containing 16:30 US open",
        "break_pct": break_pct,
        "buffer_pct": buffer_pct,
        "max_bars": max_bars,
        "signals": len(rets),
        "winrate": len(wins) / len(rets),
        "profit_factor": _profit_factor(rets),
        "avg_return": float(np.mean(rets)),
        "median_return": float(np.median(rets)),
        "max_drawdown_sum_return": _max_drawdown(equity),
        "long_signals": sum(1 for t in trades if t["side"] == "LONG"),
        "short_signals": sum(1 for t in trades if t["side"] == "SHORT"),
        "last_trades": trades[-8:],
    }


def _line_summary_pattern(res: dict) -> str:
    if not res.get("ok"):
        return f"W{res.get('window')} error: {res.get('error')}"
    if int(res.get("signals") or 0) <= 0:
        return f"W{res.get('window')} H{res.get('horizon')}: signals=0 ({res.get('message','no edge')})"
    return (
        f"W{res.get('window')} H{res.get('horizon')} K{res.get('top_k')}: "
        f"signals={res.get('signals')} WR={_fmt_pct(res.get('winrate',0))} "
        f"PF={res.get('profit_factor',0):.2f} avg={_fmt_pct(res.get('avg_return',0))} "
        f"DD={_fmt_pct(res.get('max_drawdown_sum_return',0))} "
        f"L/S={res.get('long_signals')}/{res.get('short_signals')}"
    )


def _line_summary_current(res: dict) -> str:
    if not res.get("ok"):
        return f"Current W{res.get('window')}: {res.get('error')}"
    return (
        f"Current W{res.get('window')} H{res.get('horizon')}: {res.get('suggested')} "
        f"cases={res.get('cases')} up={_fmt_pct(res.get('up_rate',0))} "
        f"avg_next={_fmt_pct(res.get('avg_future_return',0))} "
        f"best/worst={_fmt_pct(res.get('best_case',0))}/{_fmt_pct(res.get('worst_case',0))}"
    )


def _line_summary_sweep(res: dict) -> str:
    if not res.get("ok"):
        return f"US-open sweep error: {res.get('error')}"
    if int(res.get("signals") or 0) <= 0:
        return f"US-open sweep: signals=0 ({res.get('message','no trades')})"
    return (
        f"US-open sweep: signals={res.get('signals')} WR={_fmt_pct(res.get('winrate',0))} "
        f"PF={res.get('profit_factor',0):.2f} avg={_fmt_pct(res.get('avg_return',0))} "
        f"DD={_fmt_pct(res.get('max_drawdown_sum_return',0))} "
        f"L/S={res.get('long_signals')}/{res.get('short_signals')}"
    )


async def run_btc_pattern_backtest(exchange, symbol: str = "BTC_USDT", years: float = 3.0) -> tuple[str, dict]:
    started = time.time()
    candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe="4h", years=years)
    if len(candles) < 500:
        err = {"ok": False, "error": f"Недостаточно 4H свечей для теста: {len(candles)}. Нужно хотя бы 500, лучше 4000+."}
        log_event("btc_pattern_backtest_error", **err)
        return "❌ BTC pattern backtest не выполнен: " + err["error"], err
    df = _to_df(candles)
    # Drop current open candle if present.
    now_ms = int(time.time() * 1000)
    df = df[df["ts"] + TF_MS <= now_ms].reset_index(drop=True)
    date_from = datetime.fromtimestamp(int(df.iloc[0]["ts"]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    date_to = datetime.fromtimestamp(int(df.iloc[-1]["ts"]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    pattern_results = []
    current_results = []
    for w in (6, 12):
        for h in (3, 6):
            pattern_results.append(pattern_match_backtest(df, window=w, horizon=h, top_k=30, min_train=min(720, max(300, len(df)//5)), min_edge=0.60))
        current_results.append(current_pattern_stats(df, window=w, horizon=6, top_k=30))
    sweep = us_open_4h_sweep_backtest(df, break_pct=0.0015, buffer_pct=0.0005, max_bars=6)

    payload = {
        "ok": True,
        "symbol": symbol,
        "timeframe": "4h",
        "candles": int(len(df)),
        "from": date_from,
        "to": date_to,
        "years_requested": years,
        "runtime_sec": round(time.time() - started, 2),
        "pattern_results": pattern_results,
        "current_pattern": current_results,
        "us_open_sweep": sweep,
        "notes": [
            "manual backtest only; no trading logic changed",
            "pattern backtest is walk-forward: each decision uses only older candles",
            "returns include rough 0.06% round-trip cost placeholder",
            "funding-based contrarian is candle-only proxy because historical funding is not in OHLCV",
            "US-open reference candle = 15:00-19:00 MSK 4H candle containing 16:30 MSK",
        ],
    }
    log_event("btc_pattern_backtest_result", **payload)

    lines = [
        "🧪 BTC BACKTEST REPORT — 4H",
        f"Symbol: {symbol} | TF=4H | candles={len(df)} | {date_from} → {date_to}",
        "Trading logic: НЕ изменялась. Это только расчёт статистики.",
        "",
        "1️⃣ DIGITAL PATTERN MATCH 4H",
        "Окна: 6 свечей = 24h, 12 свечей = 48h. Прогноз: +3/+6 свечей.",
    ]
    for r in pattern_results:
        lines.append("- " + _line_summary_pattern(r))
    lines += ["", "Current setup by historical matches:"]
    for r in current_results:
        lines.append("- " + _line_summary_current(r))
    lines += [
        "",
        "2️⃣ US OPEN 4H FALSE BREAKOUT",
        "Reference candle: 15:00–19:00 МСК, внутри неё открытие США 16:30 МСК.",
        "Rule: break high + return inside = SHORT; break low + return inside = LONG.",
        "- " + _line_summary_sweep(sweep),
        "",
        "ИТОГ: для интеграции в торговлю смотреть PF > 1.20, WR > 55–60%, достаточно сделок и устойчивость не только в одном режиме рынка.",
        "Ошибки/сырой JSON результата: /log_full",
    ]
    return "\n".join(lines), payload


async def run_btc_pattern_backtest_1h(exchange, symbol: str = "BTC_USDT", years: float = 3.0) -> tuple[str, dict]:
    """Manual 1H digital fingerprint backtest only. No trading side effects."""
    started = time.time()
    candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe="1h", years=years, limit_per_call=500)
    tf_ms = _tf_ms("1h")
    if len(candles) < 1000:
        err = {"ok": False, "error": f"Недостаточно 1H свечей для теста: {len(candles)}. Нужно хотя бы 1000, лучше 10000+."}
        log_event("btc_pattern_backtest_1h_error", **err)
        return "❌ BTC 1H pattern backtest не выполнен: " + err["error"], err
    df = _to_df(candles)
    now_ms = int(time.time() * 1000)
    df = df[df["ts"] + tf_ms <= now_ms].reset_index(drop=True)
    date_from = datetime.fromtimestamp(int(df.iloc[0]["ts"]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    date_to = datetime.fromtimestamp(int(df.iloc[-1]["ts"]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    pattern_results = []
    current_results = []
    # Same idea as 4H, but equivalent time windows: 24 candles=24h, 48 candles=48h.
    # Forecast: +12h and +24h.
    log_event("btc_pattern_backtest_1h_progress", stage="candles_loaded", candles=int(len(df)), from_date=date_from, to_date=date_to)
    for w in (24, 48):
        for h in (12, 24):
            log_event("btc_pattern_backtest_1h_progress", stage="calc_start", window=w, horizon=h)
            pattern_results.append(pattern_match_backtest_knn(df, window=w, horizon=h, top_k=40, min_train=min(3000, max(800, len(df)//5)), min_edge=0.60, max_neighbors=600, eval_stride=3))
            log_event("btc_pattern_backtest_1h_progress", stage="calc_done", window=w, horizon=h, signals=(pattern_results[-1] or {}).get("signals"), evaluated=(pattern_results[-1] or {}).get("evaluated"))
        current_results.append(current_pattern_stats(df, window=w, horizon=24, top_k=40))

    payload = {
        "ok": True,
        "symbol": symbol,
        "timeframe": "1h",
        "candles": int(len(df)),
        "from": date_from,
        "to": date_to,
        "years_requested": years,
        "runtime_sec": round(time.time() - started, 2),
        "pattern_results": pattern_results,
        "current_pattern": current_results,
        "notes": [
            "manual 1H fingerprint backtest only; no trading logic changed",
            "walk-forward: each decision uses only older candles",
            "windows: 24 candles=24h and 48 candles=48h",
            "returns include rough 0.06% round-trip cost placeholder",
            "funding-based contrarian is candle-only proxy because historical funding is not in OHLCV",
        ],
    }
    log_event("btc_pattern_backtest_1h_result", **payload)

    lines = [
        "🧪 BTC BACKTEST REPORT — 1H",
        f"Symbol: {symbol} | TF=1H | candles={len(df)} | {date_from} → {date_to}",
        "Trading logic: НЕ изменялась. Это только расчёт статистики.",
        "",
        "1️⃣ DIGITAL PATTERN MATCH 1H",
        "Окна: 24 свечи = 24h, 48 свечей = 48h. Прогноз: +12/+24 свечей.",
    ]
    for r in pattern_results:
        lines.append("- " + _line_summary_pattern(r))
    lines += ["", "Current setup by historical matches:"]
    for r in current_results:
        lines.append("- " + _line_summary_current(r))
    lines += [
        "",
        "ИТОГ: 1H даёт больше данных, но больше шума. Для реальной торговли сравнивать с 4H и смотреть out-of-sample, PF > 1.20 и достаточное число сделок.",
        "Ошибки/сырой JSON результата: /log_full",
    ]
    return "\n".join(lines), payload


# ----------------------------
# Round-level reaction backtest
# ----------------------------

def _round_step_for_symbol(symbol: str) -> float:
    sym = str(symbol or "").upper()
    if sym.startswith("ETH"):
        return 50.0
    return 500.0


def _fmt_num(x: float) -> str:
    try:
        if abs(float(x)) >= 1000:
            return f"{float(x):,.0f}".replace(",", " ")
        return f"{float(x):.2f}"
    except Exception:
        return "n/a"


def _round_level_scan(df: pd.DataFrame, symbol: str, timeframe: str, step: float, horizon_bars: int, cooldown_bars: int, target_pct: float = 0.005) -> dict:
    """Backtest reactions around psychological round levels.

    V66 logic is deliberately simple and auditable:
    - BTC levels every 500, ETH levels every 50.
    - A resistance event happens when previous close is below the round-level zone
      and the next candle reaches that zone from below.
    - A support event happens when previous close is above the round-level zone
      and the next candle reaches that zone from above.
    - Same level/direction is ignored for `cooldown_bars` after an event.

    This avoids the old over-strict "outside full step band for 24h" rule that
    produced zero events. It is read-only and never creates/cancels orders.
    """
    if len(df) < max(300, cooldown_bars + horizon_bars + 20):
        return {"ok": False, "symbol": symbol, "timeframe": timeframe, "error": f"not enough candles: {len(df)}"}

    step = float(step)
    band_pct = 0.0015  # 0.15% zone around the round level
    min_band_abs = step * 0.05  # avoid too tiny zones on low prices
    last_hit: dict[tuple[str, int], int] = {}
    trades: list[dict] = []

    closes = df["close"].to_numpy(float)
    highs = df["high"].to_numpy(float)
    lows = df["low"].to_numpy(float)

    # Start after a small warmup so previous values are valid.  Cooldown is per level/direction.
    for i in range(1, len(df) - horizon_bars - 1):
        prev_close = float(closes[i - 1])
        hi = float(highs[i])
        lo = float(lows[i])
        close = float(closes[i])
        ts = int(df.iloc[i]["ts"])
        if not all(math.isfinite(x) and x > 0 for x in (prev_close, hi, lo, close)):
            continue

        # Candidate round levels that the candle could have approached/touched.
        level_start = max(step, math.floor((lo - step) / step) * step)
        level_end = math.ceil((hi + step) / step) * step
        levels = np.arange(level_start, level_end + step * 0.5, step)

        for level_f in levels:
            level = float(level_f)
            if level <= 0:
                continue
            band_abs = max(level * band_pct, min_band_abs)
            zone_low = level - band_abs
            zone_high = level + band_abs
            level_key = int(round(level / step))

            # Approaching resistance from below / probe of round level area.
            res_key = ("RES", level_key)
            if prev_close < zone_low and hi >= zone_low and i - last_hit.get(res_key, -10**9) >= cooldown_bars:
                entry = close
                fut_lows = lows[i + 1:i + 1 + horizon_bars]
                fut_highs = highs[i + 1:i + 1 + horizon_bars]
                if len(fut_lows) > 0:
                    max_correction = max(0.0, (entry - float(np.nanmin(fut_lows))) / max(entry, 1e-9))
                    max_adverse = max(0.0, (float(np.nanmax(fut_highs)) - entry) / max(entry, 1e-9))
                    horizon_close = float(closes[i + horizon_bars])
                    close_ret = (entry - horizon_close) / max(entry, 1e-9) - 0.0006
                    touched_round = bool(hi >= level)
                    trades.append({
                        "ts": ts,
                        "kind": "RESISTANCE_SHORT_REACTION",
                        "level": level,
                        "entry": float(entry),
                        "approach_zone_low": float(zone_low),
                        "approach_zone_high": float(zone_high),
                        "band_pct": band_pct,
                        "max_favorable": float(max_correction),
                        "max_adverse": float(max_adverse),
                        "horizon_ret": float(close_ret),
                        "success_0_5pct": bool(max_correction >= target_pct),
                        "touched_round": touched_round,
                    })
                    last_hit[res_key] = i

            # Approaching support from above / probe of round level area.
            sup_key = ("SUP", level_key)
            if prev_close > zone_high and lo <= zone_high and i - last_hit.get(sup_key, -10**9) >= cooldown_bars:
                entry = close
                fut_lows = lows[i + 1:i + 1 + horizon_bars]
                fut_highs = highs[i + 1:i + 1 + horizon_bars]
                if len(fut_highs) > 0:
                    max_bounce = max(0.0, (float(np.nanmax(fut_highs)) - entry) / max(entry, 1e-9))
                    max_adverse = max(0.0, (entry - float(np.nanmin(fut_lows))) / max(entry, 1e-9))
                    horizon_close = float(closes[i + horizon_bars])
                    close_ret = (horizon_close - entry) / max(entry, 1e-9) - 0.0006
                    touched_round = bool(lo <= level)
                    trades.append({
                        "ts": ts,
                        "kind": "SUPPORT_LONG_REACTION",
                        "level": level,
                        "entry": float(entry),
                        "approach_zone_low": float(zone_low),
                        "approach_zone_high": float(zone_high),
                        "band_pct": band_pct,
                        "max_favorable": float(max_bounce),
                        "max_adverse": float(max_adverse),
                        "horizon_ret": float(close_ret),
                        "success_0_5pct": bool(max_bounce >= target_pct),
                        "touched_round": touched_round,
                    })
                    last_hit[sup_key] = i

    def summarize(kind: str) -> dict:
        xs = [t for t in trades if t["kind"] == kind]
        if not xs:
            return {"events": 0}
        fav = [float(t["max_favorable"]) for t in xs]
        adv = [float(t["max_adverse"]) for t in xs]
        ret = [float(t["horizon_ret"]) for t in xs]
        return {
            "events": len(xs),
            "reaction_rate_0_5pct": sum(1 for t in xs if t.get("success_0_5pct")) / len(xs),
            "touched_exact_round_rate": sum(1 for t in xs if t.get("touched_round")) / len(xs),
            "avg_max_reaction": float(np.mean(fav)),
            "median_max_reaction": float(np.median(fav)),
            "avg_adverse": float(np.mean(adv)),
            "median_adverse": float(np.median(adv)),
            "avg_horizon_return_after_cost": float(np.mean(ret)),
            "profit_factor_horizon": _profit_factor(ret),
            "last_events": xs[-5:],
        }

    res = summarize("RESISTANCE_SHORT_REACTION")
    sup = summarize("SUPPORT_LONG_REACTION")
    all_rets = [float(t["horizon_ret"]) for t in trades]
    return {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "step": step,
        "band_pct": band_pct,
        "horizon_bars": horizon_bars,
        "cooldown_bars": cooldown_bars,
        "target_pct": target_pct,
        "candles": int(len(df)),
        "period_start": str(pd.to_datetime(int(df.iloc[0]["ts"]), unit="ms", utc=True)) if len(df) else None,
        "period_end": str(pd.to_datetime(int(df.iloc[-1]["ts"]), unit="ms", utc=True)) if len(df) else None,
        "events_total": len(trades),
        "resistance_from_below_short_reaction": res,
        "support_from_above_long_reaction": sup,
        "combined_profit_factor_horizon": _profit_factor(all_rets) if all_rets else 0.0,
        "combined_avg_horizon_return_after_cost": float(np.mean(all_rets)) if all_rets else 0.0,
    }



def _round_level_tp05_first_touch_scan(df: pd.DataFrame, symbol: str, timeframe: str, step: float, horizon_bars: int, cooldown_bars: int, target_pct: float = 0.005) -> dict:
    """Round-level TP 0.5% first-touch test (read-only).

    Uses the same round-level approach events as the reaction scan, but tests a
    real trade question: after entering on the touch/probe candle close, what
    happens first over the next 24h — TP +0.5% or SL?  Multiple SL variants are
    tested. If both TP and SL are touched within the same candle, the result is
    marked as SL first (conservative intrabar assumption).
    """
    if len(df) < max(300, cooldown_bars + horizon_bars + 20):
        return {"ok": False, "symbol": symbol, "timeframe": timeframe, "error": f"not enough candles: {len(df)}"}

    step = float(step)
    band_pct = 0.0015
    min_band_abs = step * 0.05
    cost = 0.0006
    sl_variants = [0.003, 0.005, 0.007, 0.010]
    closes = df["close"].to_numpy(float)
    highs = df["high"].to_numpy(float)
    lows = df["low"].to_numpy(float)
    last_hit: dict[tuple[str, int], int] = {}
    events: list[dict] = []

    for i in range(1, len(df) - horizon_bars - 1):
        prev_close = float(closes[i - 1])
        hi = float(highs[i])
        lo = float(lows[i])
        close = float(closes[i])
        ts = int(df.iloc[i]["ts"])
        if not all(math.isfinite(x) and x > 0 for x in (prev_close, hi, lo, close)):
            continue
        level_start = max(step, math.floor((lo - step) / step) * step)
        level_end = math.ceil((hi + step) / step) * step
        levels = np.arange(level_start, level_end + step * 0.5, step)
        for level_f in levels:
            level = float(level_f)
            if level <= 0:
                continue
            band_abs = max(level * band_pct, min_band_abs)
            zone_low = level - band_abs
            zone_high = level + band_abs
            level_key = int(round(level / step))
            if prev_close < zone_low and hi >= zone_low and i - last_hit.get(("RES", level_key), -10**9) >= cooldown_bars:
                events.append({"ts": ts, "i": i, "kind": "RESISTANCE_SHORT_TP05", "side": "SHORT", "level": level, "entry": close})
                last_hit[("RES", level_key)] = i
            if prev_close > zone_high and lo <= zone_high and i - last_hit.get(("SUP", level_key), -10**9) >= cooldown_bars:
                events.append({"ts": ts, "i": i, "kind": "SUPPORT_LONG_TP05", "side": "LONG", "level": level, "entry": close})
                last_hit[("SUP", level_key)] = i

    def simulate_event(ev: dict, sl_pct: float) -> dict:
        i = int(ev["i"])
        entry = float(ev["entry"])
        side = str(ev["side"])
        if side == "LONG":
            tp = entry * (1.0 + target_pct)
            sl = entry * (1.0 - sl_pct)
            for j in range(i + 1, min(len(df), i + 1 + horizon_bars)):
                hit_sl = float(lows[j]) <= sl
                hit_tp = float(highs[j]) >= tp
                # conservative: if both inside same candle, assume SL first.
                if hit_sl:
                    return {"outcome": "SL", "ret": -sl_pct - cost, "bars": j - i}
                if hit_tp:
                    return {"outcome": "TP", "ret": target_pct - cost, "bars": j - i}
            end_close = float(closes[min(len(df)-1, i + horizon_bars)])
            return {"outcome": "TIME", "ret": (end_close - entry) / max(entry, 1e-9) - cost, "bars": horizon_bars}
        else:
            tp = entry * (1.0 - target_pct)
            sl = entry * (1.0 + sl_pct)
            for j in range(i + 1, min(len(df), i + 1 + horizon_bars)):
                hit_sl = float(highs[j]) >= sl
                hit_tp = float(lows[j]) <= tp
                if hit_sl:
                    return {"outcome": "SL", "ret": -sl_pct - cost, "bars": j - i}
                if hit_tp:
                    return {"outcome": "TP", "ret": target_pct - cost, "bars": j - i}
            end_close = float(closes[min(len(df)-1, i + horizon_bars)])
            return {"outcome": "TIME", "ret": (entry - end_close) / max(entry, 1e-9) - cost, "bars": horizon_bars}

    def metrics_for(sl_pct: float, kind_filter: str | None = None) -> dict:
        xs = [e for e in events if kind_filter is None or e.get("kind") == kind_filter]
        if not xs:
            return {"events": 0, "sl_pct": sl_pct}
        sims = [simulate_event(e, sl_pct) for e in xs]
        rets = [float(x["ret"]) for x in sims]
        tp = sum(1 for x in sims if x["outcome"] == "TP")
        sl = sum(1 for x in sims if x["outcome"] == "SL")
        tm = sum(1 for x in sims if x["outcome"] == "TIME")
        return {
            "events": len(xs),
            "sl_pct": float(sl_pct),
            "tp_pct": float(target_pct),
            "tp_first_rate": tp / len(xs),
            "sl_first_rate": sl / len(xs),
            "time_exit_rate": tm / len(xs),
            "avg_return_after_cost": float(np.mean(rets)) if rets else 0.0,
            "median_return_after_cost": float(np.median(rets)) if rets else 0.0,
            "profit_factor": _profit_factor(rets),
            "max_drawdown_sum_return": _max_dd_from_returns(rets),
            "avg_bars_to_exit": float(np.mean([int(x.get("bars", 0)) for x in sims])) if sims else 0.0,
        }

    variants = []
    for slp in sl_variants:
        variants.append({
            "sl_pct": slp,
            "combined": metrics_for(slp),
            "resistance_short": metrics_for(slp, "RESISTANCE_SHORT_TP05"),
            "support_long": metrics_for(slp, "SUPPORT_LONG_TP05"),
        })
    best = None
    scored = []
    for v in variants:
        c = v.get("combined") or {}
        if int(c.get("events") or 0) <= 0:
            continue
        # PF first, then positive average return, then TP-first rate.
        score = float(c.get("profit_factor") or 0) + max(0.0, float(c.get("avg_return_after_cost") or 0) * 100.0) + float(c.get("tp_first_rate") or 0) * 0.1
        scored.append((score, v))
    if scored:
        best = sorted(scored, key=lambda x: x[0], reverse=True)[0][1]
    return {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "step": step,
        "band_pct": band_pct,
        "target_pct": target_pct,
        "horizon_bars": horizon_bars,
        "cooldown_bars": cooldown_bars,
        "candles": int(len(df)),
        "events_total": len(events),
        "sl_variants": variants,
        "best_variant": best,
        "note": "TP 0.5% first-touch; conservative intrabar assumption: if TP and SL hit in same candle, SL counts first",
    }


def _line_summary_round_tp05(res: dict) -> str:
    if not res.get("ok"):
        return f"{res.get('symbol')} {res.get('timeframe')}: error {res.get('error')}"
    best = res.get("best_variant") or {}
    c = best.get("combined") or {}
    if not c:
        return f"{res.get('symbol')} {str(res.get('timeframe')).upper()}: TP0.5 first-touch no events"
    coverage = res.get('coverage_pct')
    coverage_txt = f" coverage={_fmt_pct(coverage)}" if coverage is not None else ""
    return (
        f"{res.get('symbol')} {str(res.get('timeframe')).upper()}{coverage_txt}: "
        f"best SL={_fmt_pct(c.get('sl_pct',0))} TP=0.50% events={c.get('events',0)} "
        f"TP-first={_fmt_pct(c.get('tp_first_rate',0))} SL-first={_fmt_pct(c.get('sl_first_rate',0))} "
        f"PF={float(c.get('profit_factor') or 0):.2f} avg={_fmt_pct(c.get('avg_return_after_cost',0))} "
        f"DD={_fmt_pct(c.get('max_drawdown_sum_return',0))}"
    )

def _line_summary_round(res: dict) -> str:
    if not res.get("ok"):
        return f"{res.get('symbol')} {res.get('timeframe')}: error {res.get('error')}"
    a = res.get("resistance_from_below_short_reaction") or {}
    b = res.get("support_from_above_long_reaction") or {}
    coverage = res.get('coverage_pct')
    coverage_txt = f" coverage={_fmt_pct(coverage)}" if coverage is not None else ""
    period_txt = ""
    try:
        ps = str(res.get('period_start') or '')[:10]
        pe = str(res.get('period_end') or '')[:10]
        if ps and pe:
            period_txt = f" {ps}→{pe}"
    except Exception:
        pass
    return (
        f"{res.get('symbol')} {str(res.get('timeframe')).upper()} step={_fmt_num(res.get('step',0))}{coverage_txt}{period_txt}: "
        f"RES↘ events={a.get('events',0)} corr≥0.5={_fmt_pct(a.get('reaction_rate_0_5pct',0))} "
        f"avgCorr={_fmt_pct(a.get('avg_max_reaction',0))} avgBad={_fmt_pct(a.get('avg_adverse',0))} PF={a.get('profit_factor_horizon',0):.2f}; "
        f"SUP↗ events={b.get('events',0)} bounce≥0.5={_fmt_pct(b.get('reaction_rate_0_5pct',0))} "
        f"avgBounce={_fmt_pct(b.get('avg_max_reaction',0))} avgBad={_fmt_pct(b.get('avg_adverse',0))} PF={b.get('profit_factor_horizon',0):.2f}"
    )


async def run_round_level_backtest(exchange, years: float = 3.0, progress_cb=None) -> tuple[str, dict]:
    """Manual BTC/ETH psychological round-level reaction backtest. No trading side effects.

    progress_cb, when provided, is an async callback receiving human-readable
    status lines for Telegram progress updates.  It has no trading side effects.
    """
    async def _progress(line: str, **extra):
        try:
            log_event("round_level_backtest_progress", stage=str(line), **extra)
        except Exception:
            pass
        if progress_cb is not None:
            try:
                await progress_cb(str(line))
            except Exception as e:
                log_event("round_level_backtest_progress_cb_error", ok=False, error=str(e)[:300])

    started = time.time()
    symbols = ["BTC_USDT", "ETH_USDT"]
    # Order chosen for clear progress in chat: BTC 1H, BTC 15m, ETH 1H, ETH 15m.
    timeframes = ["1h", "15m"]
    payload_results = []
    payload_tp05_results = []
    await _progress("Round Levels started", years=years)
    for symbol in symbols:
        step = _round_step_for_symbol(symbol)
        for tf in timeframes:
            tf_label = str(tf).upper()
            try:
                candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe=tf, years=years, limit_per_call=500)
                await _progress(f"{symbol.split('_')[0]} {tf_label} loaded: {len(candles)} candles", symbol=symbol, timeframe=tf, candles=len(candles))
                if len(candles) < 500:
                    err_obj = {"ok": False, "symbol": symbol, "timeframe": tf, "error": f"not enough candles fetched: {len(candles)}"}
                    payload_results.append(err_obj)
                    payload_tp05_results.append(dict(err_obj))
                    await _progress(f"{symbol.split('_')[0]} {tf_label} skipped: not enough candles", symbol=symbol, timeframe=tf, candles=len(candles))
                    continue
                df = _to_df(candles)
                tf_ms = _tf_ms(tf)
                now_ms = int(time.time() * 1000)
                df = df[df["ts"] + tf_ms <= now_ms].reset_index(drop=True)
                if tf == "15m":
                    horizon_bars = 96   # 24h after first approach/probe
                    cooldown_bars = 96  # same level/direction only once per ~24h
                else:
                    horizon_bars = 24
                    cooldown_bars = 24

                res = _round_level_scan(df, symbol=symbol, timeframe=tf, step=step, horizon_bars=horizon_bars, cooldown_bars=cooldown_bars, target_pct=0.005)
                tp05 = _round_level_tp05_first_touch_scan(df, symbol=symbol, timeframe=tf, step=step, horizon_bars=horizon_bars, cooldown_bars=cooldown_bars, target_pct=0.005)
                try:
                    expected = max(1, int(float(years) * 365.25 * 24 * 60 * 60 * 1000 / max(tf_ms, 1)))
                    for obj in (res, tp05):
                        obj["expected_candles_approx"] = expected
                        obj["coverage_pct"] = min(1.0, len(df) / expected)
                        obj["coverage_note"] = "OK" if len(df) >= expected * 0.80 else "PARTIAL_HISTORY"
                except Exception:
                    pass
                payload_results.append(res)
                payload_tp05_results.append(tp05)
                await _progress(f"{symbol.split('_')[0]} {tf_label} calculated: events={res.get('events_total',0)} tp05_events={tp05.get('events_total',0)} coverage={_fmt_pct(res.get('coverage_pct',0))}", symbol=symbol, timeframe=tf, result_ok=bool(payload_results[-1].get("ok")), events=res.get('events_total',0), tp05_events=tp05.get('events_total',0), coverage=res.get('coverage_pct'))
            except Exception as e:
                err_obj = {"ok": False, "symbol": symbol, "timeframe": tf, "error": str(e)[:500]}
                payload_results.append(err_obj)
                payload_tp05_results.append(dict(err_obj))
                await _progress(f"{symbol.split('_')[0]} {tf_label} error: {str(e)[:160]}", symbol=symbol, timeframe=tf)

    payload = {
        "ok": True,
        "years_requested": years,
        "runtime_sec": round(time.time() - started, 2),
        "results": payload_results,
        "tp05_first_touch_results": payload_tp05_results,
        "notes": [
            "manual round-level backtest only; no trading logic changed",
            "BTC round step=500, ETH round step=50",
            "resistance: first approach/probe of band below round level -> measure correction down",
            "support: first approach/probe of band above round level -> measure bounce up",
            "15m and 1h candles are tested separately over the next 24h horizon",
            "horizon returns include rough 0.06% round-trip cost placeholder",
        ],
    }
    log_event("round_level_backtest_result", **payload)
    lines = [
        "🧪 ROUND LEVEL BACKTEST — BTC/ETH",
        f"History requested: {years:g}y | TF: 15m + 1H | Trading logic: НЕ изменялась",
        "",
        "Идея теста:",
        "- BTC: зоны к круглым уровням с шагом 500, например 74 500→75 000 или 50 500→50 000.",
        "- ETH: зоны к круглым уровням с шагом 50, например 2 950→3 000 или 2 050→2 000.",
        "- RES↘: цена впервые подходит/пробивает круглый уровень снизу — считаем коррекцию вниз за 24h.",
        "- SUP↗: цена впервые подходит/пробивает круглый уровень сверху — считаем отскок вверх за 24h.",
        "",
        "Результаты:",
    ]
    for r in payload_results:
        lines.append("- " + _line_summary_round(r))
    lines += [
        "",
        "3️⃣ TP 0.5% FIRST-TOUCH TEST",
        "Идея: после входа от круглого уровня TP=0.5%; проверяем, что сработало первым за 24h — TP или SL. SL варианты: 0.3%, 0.5%, 0.7%, 1.0%. Если TP и SL внутри одной свечи — считаем SL первым, консервативно.",
    ]
    for r in payload_tp05_results:
        lines.append("- " + _line_summary_round_tp05(r))
    lines += [
        "",
        "Как читать: corr/bounce≥0.5% = как часто был ход хотя бы 0.5%; avgCorr/avgBounce = средний лучший ход за 24h; avgBad = средний ход против; PF = грубый profit factor по закрытию через 24h.",
        "TP 0.5 first-touch = уже ближе к реальной сделке: показывает, насколько часто тейк 0.5% срабатывал раньше стопа.",
        "Сырой JSON и ошибки: /log_full",
    ]
    return "\n".join(lines), payload

# ---------------- V63 Strategy Lab backtest (manual only, no trading side effects) ----------------

def _pct_str(x: float) -> str:
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return "n/a"


def _strategy_metrics(rets: list[float]) -> dict:
    clean = [float(r) for r in (rets or []) if math.isfinite(float(r))]
    if not clean:
        return {"trades": 0, "winrate": 0.0, "profit_factor": 0.0, "avg_return": 0.0, "net_return_sum": 0.0, "max_drawdown": 0.0}
    eq = []
    s = 0.0
    for r in clean:
        s += r
        eq.append(s)
    return {
        "trades": len(clean),
        "winrate": sum(1 for r in clean if r > 0) / len(clean),
        "profit_factor": _profit_factor(clean),
        "avg_return": float(np.mean(clean)),
        "median_return": float(np.median(clean)),
        "net_return_sum": float(sum(clean)),
        "max_drawdown": float(_max_drawdown(eq)),
        "best": float(max(clean)),
        "worst": float(min(clean)),
    }


def _trade_ret(side: str, entry: float, exit_: float, cost: float = 0.0006) -> float:
    if entry <= 0 or exit_ <= 0:
        return 0.0
    if str(side).upper() == "SHORT":
        return (entry - exit_) / entry - cost
    return (exit_ - entry) / entry - cost





def _first_touch_exit_return(side: str, entry: float, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                             stop: float, take: float | None = None, cost: float = 0.0006,
                             conservative_same_bar_stop_first: bool = True) -> float:
    """Return pct using first-touch SL/TP simulation over future bars.

    If SL and TP are touched in the same candle, assume SL first by default. This is
    intentionally conservative for backtests and has no live trading side effects.
    """
    if entry <= 0 or stop <= 0 or len(closes) == 0:
        return 0.0
    is_long = str(side).upper() == "LONG"
    final_exit = float(closes[-1])
    take = float(take) if take and take > 0 else None
    for hh, ll, cc in zip(highs, lows, closes):
        hh = float(hh); ll = float(ll)
        stop_hit = (ll <= stop) if is_long else (hh >= stop)
        take_hit = False
        if take is not None:
            take_hit = (hh >= take) if is_long else (ll <= take)
        if stop_hit and take_hit:
            exit_price = stop if conservative_same_bar_stop_first else take
            return _trade_ret(side, entry, exit_price, cost=cost)
        if stop_hit:
            return _trade_ret(side, entry, stop, cost=cost)
        if take_hit:
            return _trade_ret(side, entry, take, cost=cost)
    return _trade_ret(side, entry, final_exit, cost=cost)


def _trailing_exit_return(side: str, entry: float, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                          initial_stop: float, trail_dist: float, cost: float = 0.0006) -> float:
    """Simple ATR-like trailing stop simulation; conservative stop-first by bar."""
    if entry <= 0 or initial_stop <= 0 or trail_dist <= 0 or len(closes) == 0:
        return 0.0
    is_long = str(side).upper() == "LONG"
    stop = float(initial_stop)
    best = float(entry)
    final_exit = float(closes[-1])
    for hh, ll, cc in zip(highs, lows, closes):
        hh = float(hh); ll = float(ll)
        if is_long:
            best = max(best, hh)
            stop = max(stop, best - trail_dist)
            if ll <= stop:
                return _trade_ret(side, entry, stop, cost=cost)
        else:
            best = min(best, ll)
            stop = min(stop, best + trail_dist)
            if hh >= stop:
                return _trade_ret(side, entry, stop, cost=cost)
    return _trade_ret(side, entry, final_exit, cost=cost)

def _add_strategy_indicators(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy().reset_index(drop=True)
    d["ret1"] = d["close"].pct_change().fillna(0.0)
    d["range_pct"] = ((d["high"] - d["low"]) / d["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    d["vol_ratio"] = (d["volume"] / d["volume"].rolling(30, min_periods=5).mean()).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    d["ma20"] = d["close"].rolling(20, min_periods=5).mean()
    d["ma50"] = d["close"].rolling(50, min_periods=10).mean()
    d["ma100"] = d["close"].rolling(100, min_periods=20).mean()
    d["ema12"] = d["close"].ewm(span=12, adjust=False, min_periods=5).mean()
    d["ema26"] = d["close"].ewm(span=26, adjust=False, min_periods=8).mean()
    delta = d["close"].diff().fillna(0.0)
    gain = delta.clip(lower=0).rolling(14, min_periods=5).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=5).mean().replace(0, np.nan)
    rs = gain / loss
    d["rsi14"] = (100 - (100 / (1 + rs))).replace([np.inf, -np.inf], np.nan).fillna(50.0)
    bb_mid = d["close"].rolling(20, min_periods=10).mean()
    bb_std = d["close"].rolling(20, min_periods=10).std().fillna(0.0)
    d["bb_mid"] = bb_mid
    d["bb_upper"] = bb_mid + 2.0 * bb_std
    d["bb_lower"] = bb_mid - 2.0 * bb_std
    d["bb_width"] = ((d["bb_upper"] - d["bb_lower"]) / d["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    pv = (d["close"] * d["volume"]).rolling(24, min_periods=6).sum()
    vv = d["volume"].rolling(24, min_periods=6).sum().replace(0, np.nan)
    d["rvwap24"] = (pv / vv).replace([np.inf, -np.inf], np.nan).fillna(d["close"])
    d["atr_pct"] = (d["atr14"] / d["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(d["range_pct"].rolling(20, min_periods=3).mean()).fillna(0.005)
    d["roll_high_24"] = d["high"].rolling(24, min_periods=6).max().shift(1)
    d["roll_low_24"] = d["low"].rolling(24, min_periods=6).min().shift(1)
    d["roll_high_12"] = d["high"].rolling(12, min_periods=4).max().shift(1)
    d["roll_low_12"] = d["low"].rolling(12, min_periods=4).min().shift(1)
    d["body_pct_abs"] = ((d["close"] - d["open"]).abs() / d["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return d


def _level_step_for_symbol(symbol: str) -> float:
    s = str(symbol).upper()
    return 50.0 if s.startswith("ETH") else 500.0


def _generate_strategy_returns(df_in: pd.DataFrame, symbol: str, timeframe: str, family: str, params: dict, cost: float = 0.0006) -> dict:
    """Simple, bounded strategy simulation. Entry at signal close; exit by horizon close.

    This is intentionally conservative and fast: it does not place live orders and does
    not change bot trading logic. It is a scanner to find candidates for later review.
    """
    df = _add_strategy_indicators(df_in)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float); o = df["open"].to_numpy(float)
    volr = df["vol_ratio"].to_numpy(float); atrp = df["atr_pct"].to_numpy(float)
    ma20 = df["ma20"].to_numpy(float); ma50 = df["ma50"].to_numpy(float); ma100 = df["ma100"].to_numpy(float)
    ema12 = df["ema12"].to_numpy(float); ema26 = df["ema26"].to_numpy(float); rsi14 = df["rsi14"].to_numpy(float)
    bbu = df["bb_upper"].to_numpy(float); bbl = df["bb_lower"].to_numpy(float); bbw = df["bb_width"].to_numpy(float); rvwap = df["rvwap24"].to_numpy(float)
    body_abs = df["body_pct_abs"].to_numpy(float)
    rh24 = df["roll_high_24"].to_numpy(float); rl24 = df["roll_low_24"].to_numpy(float)
    rh12 = df["roll_high_12"].to_numpy(float); rl12 = df["roll_low_12"].to_numpy(float)
    ts_arr = df["ts"].to_numpy(np.int64)
    horizon = int(params.get("horizon", 6))
    cooldown = int(params.get("cooldown", max(1, horizon // 2)))
    rets: list[float] = []
    sides: list[str] = []
    times: list[int] = []
    last_trade_i = -10**9
    n = len(df)
    start = 120
    for i in range(start, n - horizon - 1):
        if i - last_trade_i < cooldown:
            continue
        side = None
        ret_override = None
        fam = str(family)
        try:
            if fam == "momentum_breakout":
                look = int(params.get("lookback", 24)); vr = float(params.get("vol_ratio", 1.1))
                if i - look < 1: continue
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i])
                if c[i] > prev_hi and volr[i] >= vr:
                    side = "LONG"
                elif c[i] < prev_lo and volr[i] >= vr:
                    side = "SHORT"
            elif fam == "trend_pullback":
                near = float(params.get("near_atr", 0.35))
                trend_ok_long = np.isfinite(ma50[i]) and np.isfinite(ma100[i]) and ma50[i] > ma100[i] and c[i] > ma100[i]
                trend_ok_short = np.isfinite(ma50[i]) and np.isfinite(ma100[i]) and ma50[i] < ma100[i] and c[i] < ma100[i]
                if trend_ok_long and np.isfinite(ma20[i]) and l[i] <= ma20[i] * (1 + near * max(atrp[i], 0.001)) and c[i] > o[i]:
                    side = "LONG"
                elif trend_ok_short and np.isfinite(ma20[i]) and h[i] >= ma20[i] * (1 - near * max(atrp[i], 0.001)) and c[i] < o[i]:
                    side = "SHORT"
            elif fam == "super_volume_reversal":
                vr = float(params.get("vol_ratio", 1.5)); wick = float(params.get("wick", 0.45))
                rng = max(h[i]-l[i], c[i]*1e-9)
                upper = (h[i] - max(o[i], c[i])) / rng
                lower = (min(o[i], c[i]) - l[i]) / rng
                if volr[i] >= vr and lower >= wick and c[i] > o[i]:
                    side = "LONG"
                elif volr[i] >= vr and upper >= wick and c[i] < o[i]:
                    side = "SHORT"
            elif fam == "liquidity_sweep":
                buf = float(params.get("buffer", 0.0005)); look = int(params.get("lookback", 24))
                if i - look < 1: continue
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i])
                if h[i] > prev_hi * (1 + buf) and c[i] < prev_hi:
                    side = "SHORT"
                elif l[i] < prev_lo * (1 - buf) and c[i] > prev_lo:
                    side = "LONG"
            elif fam == "mean_reversion_ma":
                z = float(params.get("atr_z", 1.6))
                if np.isfinite(ma50[i]) and c[i] < ma50[i] * (1 - z * max(atrp[i], 0.001)):
                    side = "LONG"
                elif np.isfinite(ma50[i]) and c[i] > ma50[i] * (1 + z * max(atrp[i], 0.001)):
                    side = "SHORT"
            elif fam == "ma_trend_continuation":
                slope = int(params.get("slope", 5)); vr = float(params.get("vol_ratio", 1.0))
                if i - slope < 1: continue
                if np.isfinite(ma20[i]) and np.isfinite(ma50[i]) and ma20[i] > ma50[i] and ma20[i] > ma20[i-slope] and c[i] > ma20[i] and volr[i] >= vr:
                    side = "LONG"
                elif np.isfinite(ma20[i]) and np.isfinite(ma50[i]) and ma20[i] < ma50[i] and ma20[i] < ma20[i-slope] and c[i] < ma20[i] and volr[i] >= vr:
                    side = "SHORT"
            elif fam == "round_level_reversal":
                step = _level_step_for_symbol(symbol); band = float(params.get("band", 0.0015))
                level = round(c[i] / step) * step
                if level > 0:
                    # Came from below into round resistance zone with upper rejection.
                    if c[i-1] < level and h[i] >= level * (1 - band) and c[i] < level and c[i] < o[i]:
                        side = "SHORT"
                    # Came from above into round support zone with lower rejection.
                    elif c[i-1] > level and l[i] <= level * (1 + band) and c[i] > level and c[i] > o[i]:
                        side = "LONG"
            elif fam == "vwap_reversal":
                band = float(params.get("band", 0.0015))
                if np.isfinite(rvwap[i]) and l[i] < rvwap[i] * (1 - band) and c[i] > rvwap[i] and c[i] > o[i]:
                    side = "LONG"
                elif np.isfinite(rvwap[i]) and h[i] > rvwap[i] * (1 + band) and c[i] < rvwap[i] and c[i] < o[i]:
                    side = "SHORT"
            elif fam == "bollinger_squeeze_breakout":
                sq = float(params.get("squeeze", 0.75)); vr = float(params.get("vol_ratio", 1.0))
                base_w = np.nanmedian(bbw[max(0, i-80):i]) if i > 30 else np.nan
                if np.isfinite(base_w) and bbw[i] < base_w * sq and volr[i] >= vr:
                    if np.isfinite(bbu[i]) and c[i] > bbu[i]:
                        side = "LONG"
                    elif np.isfinite(bbl[i]) and c[i] < bbl[i]:
                        side = "SHORT"
            elif fam == "rsi_divergence":
                look = int(params.get("lookback", 12)); os = float(params.get("oversold", 35)); ob = float(params.get("overbought", 65))
                if i - look < 2: continue
                prev_low = np.nanmin(l[i-look:i]); prev_high = np.nanmax(h[i-look:i])
                prev_rsi_low = np.nanmin(rsi14[i-look:i]); prev_rsi_high = np.nanmax(rsi14[i-look:i])
                if l[i] < prev_low and rsi14[i] > prev_rsi_low and rsi14[i] <= os and c[i] > o[i]:
                    side = "LONG"
                elif h[i] > prev_high and rsi14[i] < prev_rsi_high and rsi14[i] >= ob and c[i] < o[i]:
                    side = "SHORT"
            elif fam == "atr_volatility_expansion":
                mult = float(params.get("atr_mult", 1.8)); vr = float(params.get("vol_ratio", 1.2))
                if (h[i] - l[i]) / max(c[i], 1e-9) >= mult * max(atrp[i], 0.001) and volr[i] >= vr:
                    side = "LONG" if c[i] > o[i] else "SHORT"
            elif fam == "funding_contrarian_proxy":
                # Candle-only proxy: funding history is not available in OHLCV backtest, so test contrarian after strong overextension.
                look = int(params.get("lookback", 8)); thr = float(params.get("move", 0.025))
                if i - look < 1: continue
                move = (c[i] - c[i-look]) / max(c[i-look], 1e-9)
                if move >= thr and np.isfinite(ma50[i]) and c[i] > ma50[i] * (1 + max(atrp[i], 0.001)):
                    side = "SHORT"
                elif move <= -thr and np.isfinite(ma50[i]) and c[i] < ma50[i] * (1 - max(atrp[i], 0.001)):
                    side = "LONG"
            elif fam == "ema_cross_trend_filter":
                if i < 2: continue
                if np.isfinite(ema12[i]) and np.isfinite(ema26[i]) and np.isfinite(ma100[i]) and ema12[i-1] <= ema26[i-1] and ema12[i] > ema26[i] and c[i] > ma100[i]:
                    side = "LONG"
                elif np.isfinite(ema12[i]) and np.isfinite(ema26[i]) and np.isfinite(ma100[i]) and ema12[i-1] >= ema26[i-1] and ema12[i] < ema26[i] and c[i] < ma100[i]:
                    side = "SHORT"
            elif fam == "donchian_breakout":
                look = int(params.get("lookback", 20)); vr = float(params.get("vol_ratio", 1.0))
                if i - look < 1: continue
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i])
                if c[i] > prev_hi and volr[i] >= vr:
                    side = "LONG"
                elif c[i] < prev_lo and volr[i] >= vr:
                    side = "SHORT"
            elif fam == "opening_range_breakout":
                # UTC 13:30 US open ≈ Moscow 16:30.  For candle data, use the first complete 1H/15m candles after US open.
                dt = datetime.fromtimestamp(int(ts_arr[i]) / 1000, tz=timezone.utc)
                if dt.hour in (14, 15, 16):
                    look = int(params.get("lookback", 4))
                    if i - look < 1: continue
                    rhi = np.nanmax(h[i-look:i]); rlo = np.nanmin(l[i-look:i])
                    if c[i] > rhi and c[i] > o[i]:
                        side = "LONG"
                    elif c[i] < rlo and c[i] < o[i]:
                        side = "SHORT"
            elif fam == "false_breakout_us_open":
                dt = datetime.fromtimestamp(int(ts_arr[i]) / 1000, tz=timezone.utc)
                if dt.hour in (13, 14, 15, 16, 17):
                    look = int(params.get("lookback", 12)); buf = float(params.get("buffer", 0.0005))
                    if i - look < 1: continue
                    prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i])
                    if h[i] > prev_hi * (1 + buf) and c[i] < prev_hi:
                        side = "SHORT"
                    elif l[i] < prev_lo * (1 - buf) and c[i] > prev_lo:
                        side = "LONG"
            elif fam == "support_resistance_retest":
                look = int(params.get("lookback", 24)); band = float(params.get("band", 0.001))
                if i - look - 2 < 1: continue
                prev_hi = np.nanmax(h[i-look-2:i-2]); prev_lo = np.nanmin(l[i-look-2:i-2])
                if c[i-1] > prev_hi and l[i] <= prev_hi * (1 + band) and c[i] > prev_hi:
                    side = "LONG"
                elif c[i-1] < prev_lo and h[i] >= prev_lo * (1 - band) and c[i] < prev_lo:
                    side = "SHORT"
            elif fam == "gap_imbalance_fill":
                mult = float(params.get("body_atr", 1.4))
                if i < 2: continue
                big_prev = body_abs[i-1] >= mult * max(atrp[i-1], 0.001)
                mid_prev = (o[i-1] + c[i-1]) / 2.0
                if big_prev and c[i-1] > o[i-1] and l[i] <= mid_prev and c[i] > mid_prev:
                    side = "LONG"
                elif big_prev and c[i-1] < o[i-1] and h[i] >= mid_prev and c[i] < mid_prev:
                    side = "SHORT"
            elif fam == "impulse_breakout_trailing":
                look = int(params.get("lookback", 24)); vr = float(params.get("vol_ratio", 1.2)); atr_mult = float(params.get("atr_mult", 1.2)); trail_mult = float(params.get("trail_atr", 1.0))
                if i - look < 1: continue
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i]); atr_abs = max(float(df.loc[i, "atr14"]), c[i] * 0.002)
                if c[i] > prev_hi and volr[i] >= vr and c[i] > o[i]:
                    side = "LONG"; stop = entry_stop = c[i] - atr_mult * atr_abs
                    ret_override = _trailing_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], entry_stop, trail_mult * atr_abs, cost=cost)
                elif c[i] < prev_lo and volr[i] >= vr and c[i] < o[i]:
                    side = "SHORT"; stop = entry_stop = c[i] + atr_mult * atr_abs
                    ret_override = _trailing_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], entry_stop, trail_mult * atr_abs, cost=cost)
            elif fam == "liquidity_sweep_2r":
                buf = float(params.get("buffer", 0.0005)); look = int(params.get("lookback", 24)); rr = float(params.get("rr", 2.0)); stop_buf_atr = float(params.get("stop_buf_atr", 0.15))
                if i - look < 1: continue
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i]); atr_abs = max(float(df.loc[i, "atr14"]), c[i]*0.002); sb = stop_buf_atr * atr_abs
                if h[i] > prev_hi * (1 + buf) and c[i] < prev_hi:
                    side = "SHORT"; stop = h[i] + sb; risk = max(stop - c[i], c[i]*0.001); take = c[i] - rr * risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
                elif l[i] < prev_lo * (1 - buf) and c[i] > prev_lo:
                    side = "LONG"; stop = l[i] - sb; risk = max(c[i] - stop, c[i]*0.001); take = c[i] + rr * risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
            elif fam == "rsi_divergence_trend":
                look = int(params.get("lookback", 24)); os = float(params.get("oversold", 38)); ob = float(params.get("overbought", 62)); rr = float(params.get("rr", 1.5)); trend = str(params.get("trend", "ma100"))
                if i - look < 2: continue
                prev_low = np.nanmin(l[i-look:i]); prev_high = np.nanmax(h[i-look:i]); prev_rsi_low = np.nanmin(rsi14[i-look:i]); prev_rsi_high = np.nanmax(rsi14[i-look:i]); atr_abs = max(float(df.loc[i, "atr14"]), c[i]*0.002)
                long_trend = (c[i] > ma100[i]) if trend == "ma100" and np.isfinite(ma100[i]) else (ma20[i] > ma50[i] if np.isfinite(ma20[i]) and np.isfinite(ma50[i]) else True)
                short_trend = (c[i] < ma100[i]) if trend == "ma100" and np.isfinite(ma100[i]) else (ma20[i] < ma50[i] if np.isfinite(ma20[i]) and np.isfinite(ma50[i]) else True)
                if long_trend and l[i] < prev_low and rsi14[i] > prev_rsi_low and rsi14[i] <= os and c[i] > o[i]:
                    side = "LONG"; stop = l[i] - 0.15*atr_abs; risk = max(c[i]-stop, c[i]*0.001); take = c[i] + rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
                elif short_trend and h[i] > prev_high and rsi14[i] < prev_rsi_high and rsi14[i] >= ob and c[i] < o[i]:
                    side = "SHORT"; stop = h[i] + 0.15*atr_abs; risk = max(stop-c[i], c[i]*0.001); take = c[i] - rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
            elif fam == "gap_imbalance_strict":
                mult = float(params.get("body_atr", 1.35)); rr = float(params.get("rr", 1.3)); vr = float(params.get("vol_ratio", 1.05))
                if i < 3: continue
                big_prev = body_abs[i-1] >= mult * max(atrp[i-1], 0.001) and volr[i-1] >= vr
                mid_prev = (o[i-1] + c[i-1]) / 2.0; atr_abs = max(float(df.loc[i, "atr14"]), c[i]*0.002)
                trend_long = np.isfinite(ma50[i]) and c[i] >= ma50[i]
                trend_short = np.isfinite(ma50[i]) and c[i] <= ma50[i]
                if big_prev and trend_long and c[i-1] > o[i-1] and l[i] <= mid_prev and c[i] > mid_prev and c[i] > o[i]:
                    side = "LONG"; stop = min(l[i], mid_prev - 0.2*atr_abs); risk = max(c[i]-stop, c[i]*0.001); take = c[i] + rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
                elif big_prev and trend_short and c[i-1] < o[i-1] and h[i] >= mid_prev and c[i] < mid_prev and c[i] < o[i]:
                    side = "SHORT"; stop = max(h[i], mid_prev + 0.2*atr_abs); risk = max(stop-c[i], c[i]*0.001); take = c[i] - rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
            elif fam == "high_volume_reversal_rr":
                vr = float(params.get("vol_ratio", 1.8)); wick = float(params.get("wick", 0.5)); rr = float(params.get("rr", 1.5))
                rng = max(h[i]-l[i], c[i]*1e-9); atr_abs = max(float(df.loc[i, "atr14"]), c[i]*0.002)
                upper = (h[i] - max(o[i], c[i])) / rng; lower = (min(o[i], c[i]) - l[i]) / rng
                if volr[i] >= vr and lower >= wick and c[i] > o[i]:
                    side = "LONG"; stop = l[i] - 0.1*atr_abs; risk = max(c[i]-stop, c[i]*0.001); take = c[i] + rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
                elif volr[i] >= vr and upper >= wick and c[i] < o[i]:
                    side = "SHORT"; stop = h[i] + 0.1*atr_abs; risk = max(stop-c[i], c[i]*0.001); take = c[i] - rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
            elif fam == "atr_expansion_trailing":
                mult = float(params.get("atr_mult", 1.7)); vr = float(params.get("vol_ratio", 1.2)); stop_mult = float(params.get("stop_atr", 1.0)); trail_mult = float(params.get("trail_atr", 1.0))
                atr_abs = max(float(df.loc[i, "atr14"]), c[i]*0.002)
                if (h[i] - l[i]) >= mult * atr_abs and volr[i] >= vr:
                    if c[i] > o[i]:
                        side = "LONG"; stop = c[i] - stop_mult*atr_abs
                    else:
                        side = "SHORT"; stop = c[i] + stop_mult*atr_abs
                    ret_override = _trailing_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, trail_mult*atr_abs, cost=cost)
            elif fam == "range_compression_breakout_rr":
                look = int(params.get("lookback", 48)); comp = float(params.get("compression", 0.8)); vr = float(params.get("vol_ratio", 1.05)); rr = float(params.get("rr", 1.5))
                if i - look < 20: continue
                range_now = (np.nanmax(h[i-look:i]) - np.nanmin(l[i-look:i])) / max(c[i], 1e-9)
                base_range = np.nanmedian(((df["high"]-df["low"])/df["close"].replace(0,np.nan)).iloc[max(0,i-look*2):i-look])
                prev_hi = np.nanmax(h[i-look:i]); prev_lo = np.nanmin(l[i-look:i]); atr_abs = max(float(df.loc[i,"atr14"]), c[i]*0.002)
                compressed = np.isfinite(base_range) and range_now <= base_range * comp
                if compressed and c[i] > prev_hi and volr[i] >= vr:
                    side = "LONG"; stop = max(prev_lo, c[i] - 1.2*atr_abs); risk = max(c[i]-stop, c[i]*0.001); take = c[i] + rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
                elif compressed and c[i] < prev_lo and volr[i] >= vr:
                    side = "SHORT"; stop = min(prev_hi, c[i] + 1.2*atr_abs); risk = max(stop-c[i], c[i]*0.001); take = c[i] - rr*risk
                    ret_override = _first_touch_exit_return(side, c[i], h[i+1:i+horizon+1], l[i+1:i+horizon+1], c[i+1:i+horizon+1], stop, take, cost=cost)
        except Exception:
            continue
        if not side:
            continue
        if ret_override is not None:
            ret = float(ret_override)
        else:
            entry = c[i]
            exit_ = c[i + horizon]
            ret = _trade_ret(side, entry, exit_, cost=cost)
        rets.append(ret); sides.append(side); times.append(int(df.loc[i, "ts"]))
        last_trade_i = i
    split_ts = int(df["ts"].iloc[int(len(df) * 0.70)]) if len(df) else 0
    train_rets = [r for r,t in zip(rets,times) if t < split_ts]
    test_rets = [r for r,t in zip(rets,times) if t >= split_ts]
    m_all = _strategy_metrics(rets); m_train = _strategy_metrics(train_rets); m_test = _strategy_metrics(test_rets)
    # Stability favors test PF, enough trades, positive test return, and modest drawdown.
    test_pf = min(float(m_test.get("profit_factor", 0.0)), 3.0)
    trade_bonus = min(float(m_test.get("trades", 0)) / 80.0, 1.0)
    dd_penalty = min(abs(float(m_test.get("max_drawdown", 0.0))) / 1.0, 1.0)
    score = test_pf * 0.55 + float(m_test.get("winrate", 0.0)) * 0.7 + trade_bonus * 0.25 + max(float(m_test.get("net_return_sum", 0.0)), 0.0) * 0.1 - dd_penalty * 0.25
    return {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "family": family,
        "params": params,
        "all": m_all,
        "train": m_train,
        "test": m_test,
        "long_signals": sum(1 for s in sides if s == "LONG"),
        "short_signals": sum(1 for s in sides if s == "SHORT"),
        "score": float(score),
    }


# ---------------- V72 Detailed strategy report for selected candidates only ----------------

def _split_ts_70(df: pd.DataFrame) -> int:
    try:
        return int(df["ts"].iloc[int(len(df) * 0.70)])
    except Exception:
        return 0


def _months_between_ms(a: int, b: int) -> float:
    try:
        if not a or not b or b <= a:
            return 0.0
        days = (int(b) - int(a)) / 1000.0 / 86400.0
        return max(days / 30.4375, 0.0)
    except Exception:
        return 0.0


def _max_loss_streak(rets: list[float]) -> int:
    best = cur = 0
    for r in rets or []:
        if float(r) <= 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return int(best)


def _trade_subset_metrics(trades: list[dict]) -> dict:
    rets = [float(t.get("ret", 0.0)) for t in (trades or []) if math.isfinite(float(t.get("ret", 0.0)))]
    m = _strategy_metrics(rets)
    m["loss_streak_max"] = _max_loss_streak(rets)
    if trades:
        ts0 = min(int(t.get("ts", 0)) for t in trades)
        ts1 = max(int(t.get("ts", 0)) for t in trades)
        months = _months_between_ms(ts0, ts1)
        net = float(m.get("net_return_sum", 0.0))
        m["period_months"] = months
        m["monthly_simple"] = net / months if months > 0 else 0.0
        m["monthly_compound"] = ((1.0 + net) ** (1.0 / months) - 1.0) if months > 0 and (1.0 + net) > 0 else 0.0
    else:
        m["period_months"] = 0.0
        m["monthly_simple"] = 0.0
        m["monthly_compound"] = 0.0
    return m


def _format_detail_metrics(m: dict) -> str:
    return (
        f"trades={int(m.get('trades',0))} WR={_pct_str(m.get('winrate',0))} "
        f"PF={float(m.get('profit_factor',0)):.2f} net={_pct_str(m.get('net_return_sum',0))} "
        f"avg={_pct_str(m.get('avg_return',0))} DD={_pct_str(m.get('max_drawdown',0))} "
        f"M≈{_pct_str(m.get('monthly_compound',0))}/mo lossStreak={int(m.get('loss_streak_max',0))}"
    )


def _generate_selected_strategy_trades(df_in: pd.DataFrame, symbol: str, timeframe: str, family: str, params: dict, cost: float = 0.0006) -> list[dict]:
    """Return detailed trades for the three selected V72 detail candidates.

    Uses the same horizon-exit logic as Strategy Lab Extra so numbers are comparable
    with the report the user already received.  Read-only backtest only.
    """
    df = _add_strategy_indicators(df_in)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float); o = df["open"].to_numpy(float)
    rsi14 = df["rsi14"].to_numpy(float)
    atrp = df["atr_pct"].to_numpy(float)
    body_abs = df["body_pct_abs"].to_numpy(float)
    ts_arr = df["ts"].to_numpy(np.int64)
    horizon = int(params.get("horizon", 6))
    cooldown = int(params.get("cooldown", max(1, horizon // 2)))
    look = int(params.get("lookback", 24))
    trades: list[dict] = []
    last_trade_i = -10**9
    n = len(df)
    for i in range(120, n - horizon - 1):
        if i - last_trade_i < cooldown:
            continue
        side = None
        reason = ""
        fam = str(family)
        try:
            if fam == "rsi_divergence":
                os = float(params.get("oversold", 38)); ob = float(params.get("overbought", 62))
                if i - look < 2:
                    continue
                prev_low = np.nanmin(l[i-look:i]); prev_high = np.nanmax(h[i-look:i])
                prev_rsi_low = np.nanmin(rsi14[i-look:i]); prev_rsi_high = np.nanmax(rsi14[i-look:i])
                if l[i] < prev_low and rsi14[i] > prev_rsi_low and rsi14[i] <= os and c[i] > o[i]:
                    side = "LONG"; reason = "bullish RSI divergence"
                elif h[i] > prev_high and rsi14[i] < prev_rsi_high and rsi14[i] >= ob and c[i] < o[i]:
                    side = "SHORT"; reason = "bearish RSI divergence"
            elif fam == "gap_imbalance_fill":
                mult = float(params.get("body_atr", 1.35))
                if i < 3:
                    continue
                big_prev = body_abs[i-1] >= mult * max(float(atrp[i-1]), 0.001)
                mid_prev = (float(o[i-1]) + float(c[i-1])) / 2.0
                if big_prev and c[i-1] > o[i-1] and l[i] <= mid_prev and c[i] > mid_prev:
                    side = "LONG"; reason = "bullish imbalance fill"
                elif big_prev and c[i-1] < o[i-1] and h[i] >= mid_prev and c[i] < mid_prev:
                    side = "SHORT"; reason = "bearish imbalance fill"
        except Exception:
            continue
        if not side:
            continue
        entry = float(c[i]); exit_ = float(c[i + horizon])
        ret = _trade_ret(side, entry, exit_, cost=cost)
        trades.append({
            "ts": int(ts_arr[i]),
            "iso": datetime.fromtimestamp(int(ts_arr[i]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "symbol": symbol,
            "timeframe": timeframe,
            "family": family,
            "params": dict(params),
            "side": side,
            "entry": entry,
            "exit": exit_,
            "ret": float(ret),
            "reason": reason,
        })
        last_trade_i = i
    return trades


def _detail_block(name: str, symbol: str, timeframe: str, family: str, params: dict, df: pd.DataFrame, trades: list[dict]) -> tuple[list[str], dict]:
    split_ts = _split_ts_70(df)
    train = [t for t in trades if int(t.get("ts", 0)) < split_ts]
    test = [t for t in trades if int(t.get("ts", 0)) >= split_ts]
    long_test = [t for t in test if t.get("side") == "LONG"]
    short_test = [t for t in test if t.get("side") == "SHORT"]
    now_ts = int(df["ts"].iloc[-1]) if len(df) else int(time.time()*1000)
    recent_6m = [t for t in trades if int(t.get("ts", 0)) >= now_ts - int(182.5*86400*1000)]
    recent_12m = [t for t in trades if int(t.get("ts", 0)) >= now_ts - int(365*86400*1000)]
    m_all = _trade_subset_metrics(trades)
    m_train = _trade_subset_metrics(train)
    m_test = _trade_subset_metrics(test)
    m_long = _trade_subset_metrics(long_test)
    m_short = _trade_subset_metrics(short_test)
    m_6m = _trade_subset_metrics(recent_6m)
    m_12m = _trade_subset_metrics(recent_12m)
    first_iso = datetime.fromtimestamp(int(df["ts"].iloc[0])/1000, tz=timezone.utc).strftime("%Y-%m-%d") if len(df) else "n/a"
    last_iso = datetime.fromtimestamp(int(df["ts"].iloc[-1])/1000, tz=timezone.utc).strftime("%Y-%m-%d") if len(df) else "n/a"
    test_months = float(m_test.get("period_months", 0.0))
    test_net = float(m_test.get("net_return_sum", 0.0))
    usd100_simple = 100.0 * (test_net / test_months) if test_months > 0 else 0.0
    usd100_comp = 100.0 * float(m_test.get("monthly_compound", 0.0))
    conclusion = "WEAK"
    if float(m_test.get("profit_factor",0)) >= 1.5 and int(m_test.get("trades",0)) >= 40 and float(m_test.get("net_return_sum",0)) > 0:
        conclusion = "DETAIL CANDIDATE"
    if float(m_test.get("profit_factor",0)) >= 2.0 and int(m_test.get("trades",0)) >= 40 and abs(float(m_test.get("max_drawdown",0))) <= 0.15:
        conclusion = "STRONG CANDIDATE"
    lines = [
        f"{name}",
        f"{symbol} {str(timeframe).upper()} {family} {params}",
        f"Data: candles={len(df)} | {first_iso}→{last_iso}",
        f"ALL:   {_format_detail_metrics(m_all)}",
        f"TRAIN: {_format_detail_metrics(m_train)}",
        f"TEST:  {_format_detail_metrics(m_test)}",
        f"TEST side split: LONG {_format_detail_metrics(m_long)} | SHORT {_format_detail_metrics(m_short)}",
        f"Recent: 6M {_format_detail_metrics(m_6m)} | 12M {_format_detail_metrics(m_12m)}",
        f"$100 estimate on TEST: ≈ ${usd100_comp:.2f}/mo compound or ${usd100_simple:.2f}/mo simple. Conclusion: {conclusion}",
    ]
    payload = {
        "name": name,
        "symbol": symbol,
        "timeframe": timeframe,
        "family": family,
        "params": params,
        "candles": len(df),
        "from": first_iso,
        "to": last_iso,
        "all": m_all,
        "train": m_train,
        "test": m_test,
        "test_long": m_long,
        "test_short": m_short,
        "recent_6m": m_6m,
        "recent_12m": m_12m,
        "test_usd100_monthly_simple": usd100_simple,
        "test_usd100_monthly_compound": usd100_comp,
        "conclusion": conclusion,
        "sample_last_trades": trades[-8:],
    }
    return lines, payload


async def run_strategy_detail_backtest(exchange, years: float = 3.0, progress_cb=None) -> tuple[str, dict]:
    """Detailed read-only check for the three selected candidates only.

    - BTC 4H RSI divergence
    - BTC 1H RSI divergence
    - ETH 1H gap imbalance fill
    """
    async def _progress(line: str, **extra):
        try:
            log_event("strategy_detail_progress", stage=str(line), **extra)
        except Exception:
            pass
        if progress_cb is not None:
            try:
                await progress_cb(str(line))
            except Exception as e:
                log_event("strategy_detail_progress_cb_error", ok=False, error=str(e)[:300])

    started = time.time()
    jobs = [
        ("1️⃣ BTC 4H RSI divergence", "BTC_USDT", "4h", "rsi_divergence", {"horizon": 6, "lookback": 6, "oversold": 38, "overbought": 62, "cooldown": 3}),
        ("2️⃣ BTC 1H RSI divergence", "BTC_USDT", "1h", "rsi_divergence", {"horizon": 12, "lookback": 48, "oversold": 38, "overbought": 62, "cooldown": 6}),
        ("3️⃣ ETH 1H gap imbalance fill", "ETH_USDT", "1h", "gap_imbalance_fill", {"horizon": 24, "body_atr": 1.35, "cooldown": 12}),
    ]
    blocks: list[str] = []
    payload_results: list[dict] = []
    errors: list[dict] = []
    await _progress("Strategy Detail started", years=years)
    for title, symbol, tf, family, params in jobs:
        label = f"{symbol.split('_')[0]} {tf.upper()} {family}"
        try:
            await _progress(f"{label} loading candles")
            candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe=tf, years=years, limit_per_call=500)
            await _progress(f"{label} loaded: {len(candles)} candles", symbol=symbol, timeframe=tf, candles=len(candles))
            if len(candles) < 700:
                errors.append({"symbol": symbol, "timeframe": tf, "error": f"not enough candles: {len(candles)}"})
                continue
            df = _to_df(candles)
            tfms = _tf_ms(tf); now_ms = int(time.time() * 1000)
            df = df[df["ts"] + tfms <= now_ms].reset_index(drop=True)
            await _progress(f"{label} calculating detailed trades")
            trades = _generate_selected_strategy_trades(df, symbol, tf, family, params)
            lines, payload = _detail_block(title, symbol, tf, family, params, df, trades)
            blocks.extend(lines + [""])
            payload_results.append(payload)
            await _progress(f"{label} calculated: trades={len(trades)}")
        except Exception as e:
            errors.append({"symbol": symbol, "timeframe": tf, "family": family, "error": str(e)[:500]})
            await _progress(f"{label} error: {str(e)[:160]}")
    payload = {
        "ok": True,
        "years_requested": years,
        "mode": "detail_selected_only",
        "runtime_sec": round(time.time() - started, 2),
        "results": payload_results,
        "errors": errors,
        "notes": [
            "manual Strategy Detail only; no trading logic changed",
            "detailed report for exactly 3 candidates selected from Strategy Lab Extra",
            "train/test split: first 70% train, last 30% test",
            "returns include rough 0.06% round-trip cost placeholder",
            "same horizon-exit logic as Strategy Lab Extra, not live trading integration",
        ],
    }
    log_event("strategy_detail_backtest_result", **payload)
    lines = [
        "🧪 STRATEGY DETAIL REPORT — SELECTED ONLY",
        f"History: {years:g}y | Trading logic: НЕ изменялась | OpenAI не вызывается",
        "Detailed modes: BTC 4H RSI divergence, BTC 1H RSI divergence, ETH 1H gap imbalance fill.",
        "Validation: train 70% / test 30%, side split, recent 6M/12M, monthly estimate from $100.",
        "",
        f"Runtime={payload['runtime_sec']}s | errors={len(errors)}",
        "",
    ] + blocks + [
        "ИТОГ: это detail-backtest, не автоторговля. Подключать только после paper/live test. Сырой JSON: /log_full",
    ]
    return "\n".join(lines[:120]), payload


def _strategy_param_grid(timeframe: str) -> list[tuple[str, dict]]:
    tf = str(timeframe).lower()
    if tf == "4h":
        horizons = [3, 6]
        looks = [6, 12, 24]
    elif tf == "15m":
        horizons = [16, 48, 96]
        looks = [24, 48, 96]
    else:
        horizons = [6, 12, 24]
        looks = [12, 24, 48]
    out: list[tuple[str, dict]] = []
    for hzn in horizons:
        for look in looks:
            out.append(("momentum_breakout", {"horizon": hzn, "lookback": look, "vol_ratio": 1.2, "cooldown": max(2, hzn//2)}))
            out.append(("liquidity_sweep", {"horizon": hzn, "lookback": look, "buffer": 0.0008, "cooldown": max(2, hzn//2)}))
    for hzn in horizons:
        out += [
            ("trend_pullback", {"horizon": hzn, "near_atr": 0.35, "cooldown": max(2, hzn//2)}),
            ("super_volume_reversal", {"horizon": hzn, "vol_ratio": 1.5, "wick": 0.45, "cooldown": max(2, hzn//2)}),
            ("mean_reversion_ma", {"horizon": hzn, "atr_z": 1.5, "cooldown": max(2, hzn//2)}),
            ("ma_trend_continuation", {"horizon": hzn, "slope": 5, "vol_ratio": 1.0, "cooldown": max(2, hzn//2)}),
            ("round_level_reversal", {"horizon": hzn, "band": 0.0015, "cooldown": max(2, hzn//2)}),
        ]
    # Bound work while still scanning many variants.
    return out[:90]




def _strategy_param_grid_extra(timeframe: str) -> list[tuple[str, dict]]:
    tf = str(timeframe).lower()
    if tf == "4h":
        horizons = [3, 6]
        looks = [6, 12, 24]
    elif tf == "15m":
        horizons = [16, 48, 96]
        looks = [16, 32, 64]
    else:
        horizons = [6, 12, 24]
        looks = [12, 24, 48]
    out: list[tuple[str, dict]] = []
    for hzn in horizons:
        cd = max(2, hzn // 2)
        out += [
            ("vwap_reversal", {"horizon": hzn, "band": 0.0015, "cooldown": cd}),
            ("bollinger_squeeze_breakout", {"horizon": hzn, "squeeze": 0.75, "vol_ratio": 1.05, "cooldown": cd}),
            ("atr_volatility_expansion", {"horizon": hzn, "atr_mult": 1.7, "vol_ratio": 1.15, "cooldown": cd}),
            ("funding_contrarian_proxy", {"horizon": hzn, "lookback": max(4, hzn), "move": 0.02, "cooldown": cd}),
            ("ema_cross_trend_filter", {"horizon": hzn, "cooldown": cd}),
            ("gap_imbalance_fill", {"horizon": hzn, "body_atr": 1.35, "cooldown": cd}),
        ]
        for look in looks:
            out += [
                ("rsi_divergence", {"horizon": hzn, "lookback": look, "oversold": 38, "overbought": 62, "cooldown": cd}),
                ("donchian_breakout", {"horizon": hzn, "lookback": look, "vol_ratio": 1.05, "cooldown": cd}),
                ("opening_range_breakout", {"horizon": hzn, "lookback": max(3, min(look, 12)), "cooldown": cd}),
                ("false_breakout_us_open", {"horizon": hzn, "lookback": max(6, min(look, 24)), "buffer": 0.0005, "cooldown": cd}),
                ("support_resistance_retest", {"horizon": hzn, "lookback": look, "band": 0.001, "cooldown": cd}),
            ]
    return out[:120]



def _strategy_param_grid_aggressive(timeframe: str) -> list[tuple[str, dict]]:
    """Aggressive read-only search with REAL exits.

    V69 adds first-touch SL/TP and trailing simulations for the high-potential
    families.  It still never changes live trading logic.
    """
    tf = str(timeframe).lower()
    if tf == "4h":
        horizons = [3, 6, 9, 12]
        looks = [6, 12, 24, 36]
    elif tf == "15m":
        horizons = [16, 32, 48, 96]
        looks = [24, 48, 96, 144]
    else:
        horizons = [6, 12, 18, 24, 36]
        looks = [12, 24, 48, 72]
    out: list[tuple[str, dict]] = []
    for hzn in horizons:
        cd = max(2, hzn // 3)
        for look in looks:
            for vr in (1.1, 1.25, 1.5):
                out.append(("impulse_breakout_trailing", {"horizon": hzn, "lookback": look, "vol_ratio": vr, "atr_mult": 1.1, "trail_atr": 1.0, "cooldown": cd}))
            for buf in (0.0003, 0.0008, 0.0015):
                for rr in (1.5, 2.0, 2.5):
                    out.append(("liquidity_sweep_2r", {"horizon": hzn, "lookback": look, "buffer": buf, "rr": rr, "stop_buf_atr": 0.15, "cooldown": cd}))
            for os, ob in ((35, 65), (38, 62), (40, 60)):
                for rr in (1.2, 1.5, 2.0):
                    out.append(("rsi_divergence_trend", {"horizon": hzn, "lookback": look, "oversold": os, "overbought": ob, "rr": rr, "trend": "ma100", "cooldown": cd}))
            for comp in (0.65, 0.8):
                for rr in (1.5, 2.0):
                    out.append(("range_compression_breakout_rr", {"horizon": hzn, "lookback": look, "compression": comp, "vol_ratio": 1.05, "rr": rr, "cooldown": cd}))
        for body in (1.1, 1.35, 1.7):
            for rr in (1.2, 1.5, 2.0):
                out.append(("gap_imbalance_strict", {"horizon": hzn, "body_atr": body, "vol_ratio": 1.05, "rr": rr, "cooldown": cd}))
        for vr in (1.6, 1.9, 2.3):
            for rr in (1.2, 1.5, 2.0):
                out.append(("high_volume_reversal_rr", {"horizon": hzn, "vol_ratio": vr, "wick": 0.50, "rr": rr, "cooldown": cd}))
        for atrm in (1.3, 1.7, 2.1):
            out.append(("atr_expansion_trailing", {"horizon": hzn, "atr_mult": atrm, "vol_ratio": 1.15, "stop_atr": 1.0, "trail_atr": 1.0, "cooldown": cd}))
        # Keep a small comparison set of old horizon-exit families so the report can compare real-exit vs horizon-exit.
        out += [
            ("ma_trend_continuation", {"horizon": hzn, "slope": 5, "vol_ratio": 1.0, "cooldown": cd}),
            ("vwap_reversal", {"horizon": hzn, "band": 0.0015, "cooldown": cd}),
            ("bollinger_squeeze_breakout", {"horizon": hzn, "squeeze": 0.60, "vol_ratio": 1.05, "cooldown": cd}),
        ]
    seen = set(); dedup: list[tuple[str, dict]] = []
    for fam, par in out:
        key = (fam, tuple(sorted(par.items())))
        if key in seen:
            continue
        seen.add(key); dedup.append((fam, par))
    return dedup[:260]

def _strategy_family_names(mode: str) -> str:
    m = str(mode).lower()
    if m.startswith("aggressive"):
        return "AGGRESSIVE REAL EXITS: impulse breakout trailing, liquidity sweep 2R, RSI divergence+trend, ETH gap imbalance strict, high-volume reversal RR, ATR trailing, range compression breakout"
    if m.startswith("extra"):
        return "VWAP reversal, Bollinger squeeze/breakout, RSI divergence, ATR volatility expansion, funding-contrarian proxy, EMA cross, Donchian breakout, opening range breakout, false breakout after US open, S/R retest, gap/imbalance fill"
    return "momentum, trend pullback, super volume, liquidity sweep, mean reversion, MA trend, round levels"

def _strategy_lab_line(r: dict) -> str:
    try:
        tm = r.get("test") or {}
        return (
            f"{r.get('symbol')} {str(r.get('timeframe')).upper()} {r.get('family')} "
            f"{r.get('params')} | test trades={tm.get('trades')} WR={_pct_str(tm.get('winrate',0))} "
            f"PF={float(tm.get('profit_factor',0)):.2f} net={_pct_str(tm.get('net_return_sum',0))} "
            f"DD={_pct_str(tm.get('max_drawdown',0))} score={float(r.get('score',0)):.2f}"
        )
    except Exception:
        return str(r)[:250]


async def run_strategy_lab_backtest(exchange, years: float = 3.0, mode: str = "safe", progress_cb=None) -> tuple[str, dict]:
    """Manual strategy lab. Read-only.  Safe mode is designed not to hang Telegram/Railway.

    mode=safe: BTC/ETH, 1H+4H, core bounded parameter grid.
    mode=full: BTC/ETH, 15m+1H+4H, core grid.
    mode=extra: BTC/ETH, 1H+4H, extended strategy families, bounded and background-progress friendly.
    mode=aggressive: BTC/ETH, 15m+1H+4H, wider bounded parameter grid to find highest-return candidates.
    """
    async def _progress(line: str, **extra):
        try:
            log_event("strategy_lab_progress", stage=str(line), **extra)
        except Exception:
            pass
        if progress_cb is not None:
            try:
                await progress_cb(str(line))
            except Exception as e:
                log_event("strategy_lab_progress_cb_error", ok=False, error=str(e)[:300])

    started = time.time()
    symbols = ["BTC_USDT", "ETH_USDT"]
    mlow = str(mode).lower()
    timeframes = ["15m", "1h", "4h"] if mlow.startswith("aggressive") or mlow in ("full", "extra_full") else ["1h", "4h"]
    all_results: list[dict] = []
    errors: list[dict] = []
    await _progress("Strategy Lab started", years=years, mode=mode)
    for symbol in symbols:
        for tf in timeframes:
            label = f"{symbol.split('_')[0]} {tf.upper()}"
            try:
                await _progress(f"{label} loading candles")
                candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe=tf, years=years, limit_per_call=500)
                await _progress(f"{label} loaded: {len(candles)} candles", symbol=symbol, timeframe=tf, candles=len(candles))
                if len(candles) < 700:
                    errors.append({"symbol": symbol, "timeframe": tf, "error": f"not enough candles: {len(candles)}"})
                    await _progress(f"{label} skipped: not enough candles")
                    continue
                df = _to_df(candles)
                tfms = _tf_ms(tf); now_ms = int(time.time() * 1000)
                df = df[df["ts"] + tfms <= now_ms].reset_index(drop=True)
                grid = _strategy_param_grid_aggressive(tf) if mlow.startswith("aggressive") else (_strategy_param_grid_extra(tf) if mlow.startswith("extra") else _strategy_param_grid(tf))
                await _progress(f"{label} calculating {len(grid)} variants")
                for family, params in grid:
                    try:
                        res = _generate_strategy_returns(df, symbol=symbol, timeframe=tf, family=family, params=params)
                        if int((res.get("test") or {}).get("trades") or 0) >= 20:
                            all_results.append(res)
                    except Exception as e:
                        errors.append({"symbol": symbol, "timeframe": tf, "family": family, "error": str(e)[:240]})
                await _progress(f"{label} calculated", variants=len(grid))
            except Exception as e:
                errors.append({"symbol": symbol, "timeframe": tf, "error": str(e)[:500]})
                await _progress(f"{label} error: {str(e)[:160]}")

    ranked = sorted(all_results, key=lambda r: (float(r.get("score", 0)), float((r.get("test") or {}).get("profit_factor", 0))), reverse=True)
    stable = [r for r in ranked if float((r.get("test") or {}).get("profit_factor", 0)) >= 1.20 and float((r.get("test") or {}).get("winrate", 0)) >= 0.52 and int((r.get("test") or {}).get("trades", 0)) >= 40 and float((r.get("test") or {}).get("net_return_sum", 0)) > 0]
    high_profit = [r for r in ranked if float((r.get("test") or {}).get("profit_factor", 0)) >= 1.50 and int((r.get("test") or {}).get("trades", 0)) >= 30 and float((r.get("test") or {}).get("net_return_sum", 0)) >= 0.35]
    payload = {
        "ok": True,
        "years_requested": years,
        "mode": mode,
        "families": _strategy_family_names(mode),
        "runtime_sec": round(time.time() - started, 2),
        "tested_variants": len(all_results),
        "errors": errors[:50],
        "top": ranked[:20],
        "stable_candidates": stable[:10],
        "high_profit_candidates": high_profit[:10],
        "notes": [
            "manual Strategy Lab only; no trading logic changed",
            "train/test split: first 70% train, last 30% test",
            "selection is by test stability, not just winrate",
            "returns include rough 0.06% round-trip cost placeholder",
            "funding-based contrarian is candle-only proxy because historical funding is not in OHLCV",
            "do not integrate unless a candidate stays strong on test period and enough trades",
        ],
    }
    log_event("strategy_lab_backtest_result", **payload)
    lines = [
        "🧪 AGGRESSIVE STRATEGY LAB — BTC/ETH" if str(mode).lower().startswith("aggressive") else ("🧪 STRATEGY LAB EXTRA BACKTEST — BTC/ETH" if str(mode).lower().startswith("extra") else "🧪 STRATEGY LAB BACKTEST — BTC/ETH"),
        f"History: {years:g}y | Mode: {mode.upper()} | Trading logic: НЕ изменялась",
        f"Tested families: {_strategy_family_names(mode)}.",
        "Validation: train 70% / test 30%, selection by PF + net + DD + trades, not only winrate.",
        "",
        f"Variants with enough test trades: {len(all_results)} | runtime={payload['runtime_sec']}s | errors={len(errors)}",
        "",
        "🏆 TOP BY STABILITY SCORE:",
    ]
    for idx, r in enumerate(ranked[:8], 1):
        lines.append(f"{idx}. " + _strategy_lab_line(r))
    lines.append("")
    lines.append("✅ STABLE CANDIDATES PF≥1.20, WR≥52%, trades≥40, net>0:")
    if stable:
        for idx, r in enumerate(stable[:5], 1):
            lines.append(f"{idx}. " + _strategy_lab_line(r))
    else:
        lines.append("Нет устойчивых кандидатов по текущим фильтрам. Не подключать в торговлю.")
    if str(mode).lower().startswith("aggressive"):
        lines.append("")
        lines.append("🔥 HIGH-PROFIT CANDIDATES PF≥1.50, trades≥30, net≥35% on test:")
        if high_profit:
            for idx, r in enumerate(high_profit[:5], 1):
                lines.append(f"{idx}. " + _strategy_lab_line(r))
        else:
            lines.append("Нет кандидатов с высокой доходностью по текущим фильтрам.")
    lines += [
        "",
        "ИТОГ: это только backtest. Подключать можно только после detail/paper test. Сырой JSON: /log_full",
    ]
    return "\n".join(lines[:80]), payload
