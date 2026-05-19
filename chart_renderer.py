from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional runtime dependency
    plt = None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _norm_symbol(symbol: str) -> str:
    s = str(symbol or "SYMBOL").replace("/", "_").replace(":", "_").replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in {"_", "-"})[:80] or "SYMBOL"


def render_trade_setup_chart(symbol: str, candles: list[list[float]], plan: Any, out_dir: str | Path | None = None) -> str | None:
    """Render a clear Telegram chart for an opened auto-trade.

    The chart is intentionally generated only after a final trade is opened,
    never during broad scanning. This keeps Railway CPU/RAM low and avoids
    spending any AI/OpenAI tokens. It draws:
    - price candles/close line;
    - entry line and marker;
    - red SL risk window;
    - green max TP reward window;
    - optional liquidity zone for liquidity_retest plans.
    """
    if plt is None:
        return None
    try:
        out_path = Path(out_dir or os.getenv("TRADE_CHART_DIR", "/tmp/trade_charts"))
        out_path.mkdir(parents=True, exist_ok=True)
        rows = list(candles or [])[-120:]
        if len(rows) < 5:
            return None
        xs = list(range(len(rows)))
        opens = [_safe_float(c[1]) for c in rows]
        highs = [_safe_float(c[2]) for c in rows]
        lows = [_safe_float(c[3]) for c in rows]
        closes = [_safe_float(c[4]) for c in rows]

        entry = _safe_float(getattr(plan, "entry_price", 0))
        sl = _safe_float(getattr(plan, "stop_price", 0))
        tp = _safe_float(getattr(plan, "take_price", 0))
        side = str(getattr(plan, "side", "")).upper()
        strategy = str(getattr(plan, "strategy", ""))
        rr = _safe_float(getattr(plan, "liquidity_retest_rr", 0))
        zone_low = _safe_float(getattr(plan, "liquidity_retest_zone_low", 0))
        zone_high = _safe_float(getattr(plan, "liquidity_retest_zone_high", 0))
        confidence = _safe_float(getattr(plan, "confidence", 0))

        if entry <= 0 or sl <= 0 or tp <= 0 or side not in {"LONG", "SHORT"}:
            return None

        fig, ax = plt.subplots(figsize=(13, 7.2))
        fig.patch.set_facecolor("white")
        ax.set_facecolor("#fbfbfb")

        # readable candle bars: wick + body
        width = 0.55
        for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
            up = c >= o
            body_color = "#1f9d55" if up else "#c92a2a"
            ax.vlines(i, l, h, color=body_color, linewidth=0.8, alpha=0.75)
            bottom = min(o, c)
            height = max(abs(c - o), max(max(highs) - min(lows), 1e-12) * 0.001)
            ax.add_patch(plt.Rectangle((i - width / 2, bottom), width, height, color=body_color, alpha=0.60))
        ax.plot(xs, closes, color="#111827", linewidth=1.25, alpha=0.80, label="Close")

        x0, x1 = -1, len(xs)
        # Risk/reward windows. Green is profit side, red is stop side.
        if side == "LONG":
            ax.axhspan(min(sl, entry), max(sl, entry), color="#ff4d4f", alpha=0.22, label="STOP risk window")
            ax.axhspan(min(entry, tp), max(entry, tp), color="#22c55e", alpha=0.18, label="MAX TAKE window")
        else:
            ax.axhspan(min(entry, sl), max(entry, sl), color="#ff4d4f", alpha=0.22, label="STOP risk window")
            ax.axhspan(min(tp, entry), max(tp, entry), color="#22c55e", alpha=0.18, label="MAX TAKE window")

        if zone_low > 0 and zone_high > zone_low:
            ax.axhspan(zone_low, zone_high, color="#f59e0b", alpha=0.15, label="Liquidity zone")

        # Lines + large right-side labels
        levels = [(entry, "ENTRY", "#2563eb", 2.2), (sl, "STOP", "#dc2626", 2.0), (tp, "MAX TAKE", "#16a34a", 2.0)]
        for y, label, color, lw in levels:
            ax.hlines(y, x0, x1, colors=color, linestyles="-" if label == "ENTRY" else "--", linewidth=lw)
            ax.text(x1, y, f"  {label} {y:.8g}", va="center", ha="left", fontsize=12, fontweight="bold", color=color,
                    bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor=color, alpha=0.90))

        # Entry marker on last candle area.
        ax.scatter([len(xs) - 1], [entry], s=130, color="#2563eb", edgecolors="white", linewidths=1.5, zorder=5)
        ax.text(len(xs) - 1, entry, "  ENTRY", va="bottom", ha="left", fontsize=11, fontweight="bold", color="#2563eb")

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr_calc = reward / risk if risk > 0 else 0.0
        notional = _safe_float(getattr(plan, "planned_notional_usdt", 0))
        lev = int(_safe_float(getattr(plan, "leverage", 0)))
        title = f"{symbol} {side} | {strategy} | RR {rr or rr_calc:.2f} | conf {confidence:.1f}"
        subtitle = f"Entry {entry:.8g}  SL {sl:.8g}  TP {tp:.8g}  Notional ~{notional:.2f} USDT" + (f"  Lev {lev}x" if lev else "")
        ax.set_title(title + "\n" + subtitle, fontsize=15, fontweight="bold")
        ax.grid(True, alpha=0.18)
        ax.legend(loc="upper left", fontsize=9)
        ax.margins(x=0.04, y=0.10)
        fig.tight_layout()
        path = out_path / f"trade_{_norm_symbol(symbol)}_{int(time.time()*1000)}.png"
        fig.savefig(path, dpi=170)
        plt.close(fig)
        return str(path)
    except Exception:
        try:
            plt.close("all")
        except Exception:
            pass
        return None
