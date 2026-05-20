from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

AI_SCALPING_STRATEGY = "ai_scalping"


def _base(symbol: str | None) -> str:
    s = str(symbol or "").upper().replace("_", "/")
    if s.startswith("BTC"):
        return "BTC"
    if s.startswith("ETH"):
        return "ETH"
    return s.split("/")[0].split(":")[0] if s else "-"


def _side(side: str | None) -> str:
    s = str(side or "").upper()
    return s if s in {"LONG", "SHORT"} else "-"


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


@dataclass
class AIStatsSummary:
    scope: str
    session_id: int
    reset_at: float
    total_trades: int
    wins: int
    losses: int
    winrate: float
    profit_factor: float
    pnl_usdt: float
    wait_count: int
    avg_hold_sec: float
    by_symbol: dict[str, dict[str, Any]]
    by_side: dict[str, dict[str, Any]]


class AIStatsManager:
    """Real stats for the AI BTC/ETH scalping mode.

    Trades are read from the durable `trades` table. WAIT decisions are read
    from the durable `ai_scalping_events` table. This class does not affect
    execution, order placement, risk, TP/SL, or the AI prompt.
    """

    def __init__(self, storage):
        self.storage = storage

    async def record_wait(self, symbol: str, reason: str = "", confidence: float = 0.0, model: str = "") -> None:
        sid = int(await self.storage.get("ai_scalping_session_id", 1) or 1)
        await self.storage.add_ai_scalping_event({
            "ts": time.time(),
            "session_id": sid,
            "symbol": symbol,
            "event": "WAIT",
            "reason": reason,
            "confidence": confidence,
            "model": model,
        })

    async def reset_session(self) -> int:
        sid = int(await self.storage.increment_counter("ai_scalping_session_id", 1))
        await self.storage.set("ai_scalping_session_reset_at", time.time())
        return sid

    async def summary(self, scope: str = "current") -> AIStatsSummary:
        scope = (scope or "current").lower()
        sid = int(await self.storage.get("ai_scalping_session_id", 1) or 1)
        reset_at = _f(await self.storage.get("ai_scalping_session_reset_at", 0.0), 0.0)
        since = reset_at if scope == "current" else None
        trades = [t for t in await self.storage.trade_rows(since=since) if str(t.get("strategy", "")).lower() == AI_SCALPING_STRATEGY]
        events = await self.storage.ai_scalping_events(since=since)
        waits = [e for e in events if str(e.get("event", "")).upper() == "WAIT"]

        wins = [t for t in trades if _f(t.get("pnl_usdt")) > 0 or str(t.get("result", "")).lower() == "win"]
        losses = [t for t in trades if _f(t.get("pnl_usdt")) <= 0 and str(t.get("result", "")).lower() != "win"]
        gross_win = sum(max(0.0, _f(t.get("pnl_usdt"))) for t in trades)
        gross_loss = abs(sum(min(0.0, _f(t.get("pnl_usdt"))) for t in trades))
        pf = gross_win / gross_loss if gross_loss > 0 else (gross_win if gross_win > 0 else 0.0)
        total = len(trades)
        wr = (len(wins) / total * 100.0) if total else 0.0
        hold_values = []
        for t in trades:
            op = _f(t.get("ts_open"), 0.0)
            cl = _f(t.get("ts_close"), 0.0)
            if op > 0 and cl >= op:
                hold_values.append(cl - op)
        avg_hold = sum(hold_values) / len(hold_values) if hold_values else 0.0

        def bucket(items, key_fn):
            out: dict[str, dict[str, Any]] = {}
            for item in items:
                k = key_fn(item)
                if k not in out:
                    out[k] = {"trades": 0, "wins": 0, "losses": 0, "pnl_usdt": 0.0, "winrate": 0.0}
                out[k]["trades"] += 1
                pnl = _f(item.get("pnl_usdt"))
                out[k]["pnl_usdt"] += pnl
                if pnl > 0 or str(item.get("result", "")).lower() == "win":
                    out[k]["wins"] += 1
                else:
                    out[k]["losses"] += 1
            for v in out.values():
                v["winrate"] = (v["wins"] / v["trades"] * 100.0) if v["trades"] else 0.0
                v["pnl_usdt"] = round(v["pnl_usdt"], 6)
            return out

        return AIStatsSummary(
            scope=scope,
            session_id=sid,
            reset_at=reset_at,
            total_trades=total,
            wins=len(wins),
            losses=len(losses),
            winrate=wr,
            profit_factor=pf,
            pnl_usdt=sum(_f(t.get("pnl_usdt")) for t in trades),
            wait_count=len(waits),
            avg_hold_sec=avg_hold,
            by_symbol=bucket(trades, lambda t: _base(t.get("symbol"))),
            by_side=bucket(trades, lambda t: _side(t.get("side"))),
        )

    @staticmethod
    def format(summary: AIStatsSummary) -> str:
        title = "Current session" if summary.scope == "current" else "Lifetime"
        def bline(title: str, data: dict[str, Any]) -> str:
            if not data:
                return f"{title}: -"
            parts = []
            for k in sorted(data.keys()):
                v = data[k]
                parts.append(f"{k} {v['winrate']:.1f}% ({v['wins']}/{v['trades']}) pnl={v['pnl_usdt']:.3f}")
            return f"{title}: " + " | ".join(parts)
        return (
            f"📊 AI Scalping Stats — {title}\n"
            f"Session ID: {summary.session_id}\n"
            f"Trades: {summary.total_trades}\n"
            f"Wins/Losses: {summary.wins}/{summary.losses}\n"
            f"Winrate: {summary.winrate:.1f}%\n"
            f"Profit factor: {summary.profit_factor:.2f}\n"
            f"PnL: {summary.pnl_usdt:.4f} USDT\n"
            f"WAIT count: {summary.wait_count}\n"
            f"Avg hold: {summary.avg_hold_sec:.1f}s\n"
            f"{bline('BTC/ETH', summary.by_symbol)}\n"
            f"{bline('LONG/SHORT', summary.by_side)}"
        )
