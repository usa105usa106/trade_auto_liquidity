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
        for _ in range(80):  # 80*200 4h candles > 7 years; safety cap.
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
            while cur_sec < end_all_sec and len(rows) < 20000:
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
    for w in (24, 48):
        for h in (12, 24):
            pattern_results.append(pattern_match_backtest(df, window=w, horizon=h, top_k=40, min_train=min(3000, max(800, len(df)//5)), min_edge=0.60))
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
    """Backtest first approach/probe of psychological round levels.

    Resistance case: price approaches a round level from below.  Example BTC 74500-75000.
    Support case: price approaches a round level from above.     Example BTC 50500-50000.

    This is intentionally read-only and statistical.  It does not create orders.
    """
    if len(df) < max(300, cooldown_bars + horizon_bars + 20):
        return {"ok": False, "symbol": symbol, "timeframe": timeframe, "error": f"not enough candles: {len(df)}"}
    step = float(step)
    lower_hits: dict[tuple[str, int], int] = {}
    trades: list[dict] = []
    closes = df["close"].to_numpy(float)
    highs = df["high"].to_numpy(float)
    lows = df["low"].to_numpy(float)
    # Start after cooldown so "first approach" has a lookback.
    for i in range(max(2, cooldown_bars), len(df) - horizon_bars - 1):
        prev_close = closes[i - 1]
        hi = highs[i]
        lo = lows[i]
        close = closes[i]
        ts = int(df.iloc[i]["ts"])

        # Rising into resistance: nearest round level above previous close.
        res_level = math.ceil(prev_close / step) * step
        res_band_low = res_level - step
        res_key = ("RES", int(res_level))
        # First touch of the 1-step band below the round level after being outside it for cooldown.
        res_prev_outside = np.nanmax(highs[i - cooldown_bars:i]) < res_band_low
        res_event = hi >= res_band_low and close < res_level * 1.002 and res_prev_outside
        if res_event and i - lower_hits.get(res_key, -10**9) >= cooldown_bars:
            entry = close
            fut_lows = lows[i + 1:i + 1 + horizon_bars]
            fut_highs = highs[i + 1:i + 1 + horizon_bars]
            if len(fut_lows) > 0:
                max_correction = max(0.0, (entry - float(np.nanmin(fut_lows))) / max(entry, 1e-9))
                max_adverse = max(0.0, (float(np.nanmax(fut_highs)) - entry) / max(entry, 1e-9))
                close_ret = (entry - closes[i + horizon_bars]) / max(entry, 1e-9) - 0.0006
                touched_round = bool(hi >= res_level)
                trades.append({
                    "ts": ts, "kind": "RESISTANCE_SHORT_REACTION", "level": float(res_level), "entry": float(entry),
                    "max_favorable": float(max_correction), "max_adverse": float(max_adverse), "horizon_ret": float(close_ret),
                    "success_0_5pct": bool(max_correction >= target_pct), "touched_round": touched_round,
                })
                lower_hits[res_key] = i

        # Falling into support: nearest round level below previous close.
        sup_level = math.floor(prev_close / step) * step
        sup_band_high = sup_level + step
        sup_key = ("SUP", int(sup_level))
        sup_prev_outside = np.nanmin(lows[i - cooldown_bars:i]) > sup_band_high
        sup_event = lo <= sup_band_high and close > sup_level * 0.998 and sup_prev_outside
        if sup_event and i - lower_hits.get(sup_key, -10**9) >= cooldown_bars:
            entry = close
            fut_lows = lows[i + 1:i + 1 + horizon_bars]
            fut_highs = highs[i + 1:i + 1 + horizon_bars]
            if len(fut_highs) > 0:
                max_bounce = max(0.0, (float(np.nanmax(fut_highs)) - entry) / max(entry, 1e-9))
                max_adverse = max(0.0, (entry - float(np.nanmin(fut_lows))) / max(entry, 1e-9))
                close_ret = (closes[i + horizon_bars] - entry) / max(entry, 1e-9) - 0.0006
                touched_round = bool(lo <= sup_level)
                trades.append({
                    "ts": ts, "kind": "SUPPORT_LONG_REACTION", "level": float(sup_level), "entry": float(entry),
                    "max_favorable": float(max_bounce), "max_adverse": float(max_adverse), "horizon_ret": float(close_ret),
                    "success_0_5pct": bool(max_bounce >= target_pct), "touched_round": touched_round,
                })
                lower_hits[sup_key] = i

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
        "horizon_bars": horizon_bars,
        "cooldown_bars": cooldown_bars,
        "target_pct": target_pct,
        "candles": int(len(df)),
        "events_total": len(trades),
        "resistance_from_below_short_reaction": res,
        "support_from_above_long_reaction": sup,
        "combined_profit_factor_horizon": _profit_factor(all_rets) if all_rets else 0.0,
        "combined_avg_horizon_return_after_cost": float(np.mean(all_rets)) if all_rets else 0.0,
    }


def _line_summary_round(res: dict) -> str:
    if not res.get("ok"):
        return f"{res.get('symbol')} {res.get('timeframe')}: error {res.get('error')}"
    a = res.get("resistance_from_below_short_reaction") or {}
    b = res.get("support_from_above_long_reaction") or {}
    return (
        f"{res.get('symbol')} {str(res.get('timeframe')).upper()} step={_fmt_num(res.get('step',0))}: "
        f"RES↘ events={a.get('events',0)} corr≥0.5={_fmt_pct(a.get('reaction_rate_0_5pct',0))} "
        f"avgCorr={_fmt_pct(a.get('avg_max_reaction',0))} avgBad={_fmt_pct(a.get('avg_adverse',0))} PF={a.get('profit_factor_horizon',0):.2f}; "
        f"SUP↗ events={b.get('events',0)} bounce≥0.5={_fmt_pct(b.get('reaction_rate_0_5pct',0))} "
        f"avgBounce={_fmt_pct(b.get('avg_max_reaction',0))} avgBad={_fmt_pct(b.get('avg_adverse',0))} PF={b.get('profit_factor_horizon',0):.2f}"
    )


async def run_round_level_backtest(exchange, years: float = 3.0) -> tuple[str, dict]:
    """Manual BTC/ETH psychological round-level reaction backtest. No trading side effects."""
    started = time.time()
    symbols = ["BTC_USDT", "ETH_USDT"]
    timeframes = ["15m", "1h"]
    payload_results = []
    for symbol in symbols:
        step = _round_step_for_symbol(symbol)
        for tf in timeframes:
            try:
                candles = await fetch_ohlcv_history(exchange, symbol=symbol, timeframe=tf, years=years, limit_per_call=500)
                if len(candles) < 500:
                    payload_results.append({"ok": False, "symbol": symbol, "timeframe": tf, "error": f"not enough candles fetched: {len(candles)}"})
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
                payload_results.append(_round_level_scan(df, symbol=symbol, timeframe=tf, step=step, horizon_bars=horizon_bars, cooldown_bars=cooldown_bars, target_pct=0.005))
            except Exception as e:
                payload_results.append({"ok": False, "symbol": symbol, "timeframe": tf, "error": str(e)[:500]})

    payload = {
        "ok": True,
        "years_requested": years,
        "runtime_sec": round(time.time() - started, 2),
        "results": payload_results,
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
        "Как читать: corr/bounce≥0.5% = как часто был ход хотя бы 0.5%; avgCorr/avgBounce = средний лучший ход за 24h; avgBad = средний ход против; PF = грубый profit factor по закрытию через 24h.",
        "Сырой JSON и ошибки: /log_full",
    ]
    return "\n".join(lines), payload
