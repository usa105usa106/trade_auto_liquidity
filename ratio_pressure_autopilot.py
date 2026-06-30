from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Awaitable

from models import TradePlan
from debug_log import log_event

RATIO_STRATEGY = "ratio_pressure_1h"
RATIO_ENABLED_KEY = "ratio_pressure_autopilot_enabled"
RATIO_LAST_RUN_KEY = "ratio_pressure_last_run_ts"
RATIO_STATUS_MSG_KEY = "ratio_pressure_status"
RATIO_ENABLED_SINCE_KEY = "ratio_pressure_enabled_since_ts"
RATIO_NEXT_SCAN_KEY = "ratio_pressure_next_scan_ts"


@dataclass
class RatioSignal:
    symbol: str
    side: str
    entry_reference: float
    stop_price: float
    take_price: float
    vol_rank: float
    range_rank: float
    ret3_rank: float
    ret3_pct: float
    range_pct: float
    vol_z: float
    candle_ts: int
    reason: str


class RatioPressureAutopilot:
    """Mechanical ETH/BTC 1h ratio_pressure_afterimage autopilot.

    This mode deliberately has no AI dependency.  It only calculates the final
    live-candidate signal on closed 1h candles and reuses the bot's existing
    ExecutionEngine for MEXC entry + exchange-side TP/SL placement.
    """

    symbols = ("ETH_USDT", "BTC_USDT")
    timeframe = "1h"
    strategy = RATIO_STRATEGY

    def __init__(self, storage, exchange_client, execution_engine, notify: Callable[[str], Awaitable[Any]] | None = None, status_notify: Callable[[str], Awaitable[Any]] | None = None):
        self.storage = storage
        self.exchange_client = exchange_client
        self.execution_engine = execution_engine
        self.notify = notify
        self.status_notify = status_notify or notify
        self._running = False
        self._last_heartbeat_ts = 0.0
        self._last_wait_info: dict[str, dict] = {}

    @staticmethod
    def _truthy(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "да"}

    async def _setting(self, settings: dict, key: str, default: Any) -> Any:
        if key in settings:
            return settings.get(key)
        env_key = key.upper()
        return os.getenv(env_key, default)

    async def _notify(self, text: str) -> None:
        if not self.notify:
            return
        try:
            await self.notify(str(text)[:3900])
        except Exception as e:
            log_event("ratio_pressure_notify_failed", ok=False, error=str(e)[:300])

    async def _notify_status(self, text: str) -> None:
        """Replaceable bottom status card for WAIT/scan lifecycle; avoids chat trash."""
        if not self.status_notify:
            return
        try:
            await self.status_notify(str(text)[:3900])
        except Exception as e:
            log_event("ratio_pressure_status_notify_failed", ok=False, error=str(e)[:300])

    def stop(self) -> None:
        self._running = False

    @staticmethod
    def next_1h_close_ts(now: float | None = None, delay_sec: int = 65) -> float:
        now_dt = datetime.fromtimestamp(now or time.time(), tz=timezone.utc)
        nxt = (now_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        return nxt.timestamp() + float(delay_sec)


    @staticmethod
    def current_1h_scan_ts(now: float | None = None, delay_sec: int = 65) -> float:
        """Scheduled scan timestamp for the current UTC hour.

        Kept for diagnostics/tests only.  Live scheduling uses an explicit
        stored next scan timestamp created at the moment the mode is enabled.
        This is critical: if the user enables Ratio at 06:55 MSK, next scan is
        07:01 MSK; if enabled at 07:03 MSK, next scan is 08:01 MSK.
        """
        now_dt = datetime.fromtimestamp(now or time.time(), tz=timezone.utc)
        cur = now_dt.replace(minute=0, second=0, microsecond=0)
        return cur.timestamp() + float(delay_sec)

    @classmethod
    def first_scan_after_enable_ts(cls, enabled_at: float | None = None, delay_sec: int = 65) -> float:
        """First future scan after the user turns the mode ON.

        No catch-up is allowed on enable.
        - enabled before HH:01:05 => scan at HH:01:05;
        - enabled after HH:01:05 => scan at next hour HH+1:01:05.
        """
        n = float(enabled_at or time.time())
        cur_scan = cls.current_1h_scan_ts(n, delay_sec=delay_sec)
        if n < cur_scan:
            return cur_scan
        return cur_scan + 3600.0

    @classmethod
    def next_scan_after_ts(cls, after_ts: float | None = None, delay_sec: int = 65) -> float:
        n = float(after_ts or time.time())
        cur_scan = cls.current_1h_scan_ts(n, delay_sec=delay_sec)
        if n < cur_scan:
            return cur_scan
        return cur_scan + 3600.0

    @classmethod
    def upcoming_or_due_1h_scan_ts(cls, now: float | None = None, delay_sec: int = 65, last_run_ts: float | int | None = None) -> float:
        """Backward-compatible display helper: always show the next future scan.

        Older builds used this as catch-up logic; Ratio live now stores
        `ratio_pressure_next_scan_ts` explicitly on enable.
        """
        return cls.next_1h_close_ts(now=now or time.time(), delay_sec=delay_sec)

    @staticmethod
    def _fmt_utc(ts: float | int | None) -> str:
        """Telegram display time for Ratio mode: MSK (UTC+3).

        Important: scheduler/calculations remain UTC timestamps; only user-visible
        formatting is shifted to Moscow time to avoid changing trading logic.
        """
        if not ts:
            return "-"
        return datetime.fromtimestamp(float(ts), tz=timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M МСК")

    def status_text(self, settings: dict | None = None) -> str:
        s = settings or {}
        enabled = self._truthy(s.get(RATIO_ENABLED_KEY), False)
        delay = int(float(s.get("ratio_pressure_delay_after_hour_sec", 65) or 65))
        next_ts = float(s.get(RATIO_NEXT_SCAN_KEY) or self.next_scan_after_ts(delay_sec=delay))
        return (
            "🧬 ETH/BTC 1h/1h Ratio Pressure\n\n"
            f"Статус: {'ВКЛ' if enabled else 'ВЫКЛ'}\n"
            "Режим: FINAL_LIVE_CANDIDATE\n"
            "Сигнал: vol_z + range_vol + ret3\n"
            "ETH: 10% баланса x10\n"
            "BTC: 5% баланса x10\n"
            "SL: 1% | TP: 6R = 6% цены\n"
            "Max: 2 позиции, max 1 на символ\n"
            "ИИ: не используется\n"
            "Скан: только автоматически после закрытия 1H свечи\n"
            "Sync: ~1 сек, без сообщений при обычной синхронизации\n"
            f"Следующий 1H scan: {self._fmt_utc(next_ts)}\n\n"
            "Исполнение: через существующий ExecutionEngine, как в Claude/ChatGPT executor."
        )

    async def run_loop(self, app=None) -> None:
        self._running = True
        while self._running:
            try:
                settings = await self.storage.all_settings()
                if not self._truthy(settings.get(RATIO_ENABLED_KEY), False):
                    await asyncio.sleep(10)
                    continue

                delay = int(float(settings.get("ratio_pressure_delay_after_hour_sec", 65) or 65))
                now = time.time()
                try:
                    last_run = float(settings.get(RATIO_LAST_RUN_KEY) or 0.0)
                except Exception:
                    last_run = 0.0
                try:
                    target = float(settings.get(RATIO_NEXT_SCAN_KEY) or 0.0)
                except Exception:
                    target = 0.0
                if target <= 0:
                    target = self.first_scan_after_enable_ts(now, delay_sec=delay)
                    await self.storage.set(RATIO_NEXT_SCAN_KEY, target, bump_revision=False)
                    log_event("ratio_pressure_next_scan_initialized", ok=True, next_scan_msk=self._fmt_utc(target))

                # The scan slot is fixed when the mode is enabled.
                # Enabled before 07:01 -> target 07:01 and it runs then.
                # Enabled after 07:01 -> target 08:01. No stale catch-up from 07:01.
                if now >= target and last_run < target - 5:
                    max_lag = int(float(settings.get("ratio_pressure_max_scan_lag_sec", 15 * 60) or (15 * 60)))
                    if now - target > max_lag:
                        skipped = target
                        target = self.next_scan_after_ts(now, delay_sec=delay)
                        await self.storage.set(RATIO_NEXT_SCAN_KEY, target, bump_revision=False)
                        log_event("ratio_pressure_stale_scan_skipped", ok=False, skipped_scan_msk=self._fmt_utc(skipped), next_scan_msk=self._fmt_utc(target), lag_sec=round(now-skipped, 1))
                        await asyncio.sleep(min(30.0, max(1.0, target - now)))
                        continue
                    log_event("ratio_pressure_scan_due", ok=True, due_scan_msk=self._fmt_utc(target), last_run_msk=self._fmt_utc(last_run) if last_run else "-")
                    await self.cycle(app=app, trigger="schedule")
                    await self.storage.set(RATIO_LAST_RUN_KEY, target, bump_revision=False)
                    next_target = target + 3600.0
                    while next_target <= time.time():
                        next_target += 3600.0
                    await self.storage.set(RATIO_NEXT_SCAN_KEY, next_target, bump_revision=False)
                    log_event("ratio_pressure_next_scan_scheduled", ok=True, next_scan_msk=self._fmt_utc(next_target))
                    await asyncio.sleep(60)
                    continue

                if now - self._last_heartbeat_ts > 3500:
                    self._last_heartbeat_ts = now
                    log_event("ratio_pressure_loop_wait", ok=True, next_scan_msk=self._fmt_utc(target), last_run_msk=self._fmt_utc(last_run) if last_run else "-")
                await asyncio.sleep(min(30.0, max(1.0, target - now)))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event("ratio_pressure_loop_error", ok=False, error=str(e)[:800])
                await self._notify(f"❌ Ratio Pressure loop error: {str(e)[:900]}")
                await asyncio.sleep(30)

    async def _fetch_closed_1h(self, symbol: str, limit: int = 200) -> list[list[float]]:
        rows = await self.exchange_client.fetch_ohlcv(symbol, timeframe="1h", limit=limit)
        now_ms = int(time.time() * 1000)
        closed = []
        for r in rows or []:
            try:
                ts = int(float(r[0]))
                if ts < 10_000_000_000:
                    ts *= 1000
                # 1h candle is usable only after its hour has fully closed.
                if ts + 3600_000 <= now_ms:
                    closed.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] if len(r) > 5 else 0.0)])
            except Exception:
                continue
        return closed[-limit:]

    @staticmethod
    def _mean_std(vals: list[float]) -> tuple[float, float]:
        vals = [float(x) for x in vals if math.isfinite(float(x))]
        if not vals:
            return 0.0, 0.0
        m = sum(vals) / len(vals)
        if len(vals) < 2:
            return m, 0.0
        var = sum((x - m) ** 2 for x in vals) / max(1, len(vals) - 1)
        return m, math.sqrt(max(0.0, var))

    @staticmethod
    def _rank(value: float, history: list[float]) -> float:
        """Tie-safe percentile rank in [0, 1].

        Important for live: if the current value equals most of the history
        (flat volume/range/ret3), it must be neutral (~0.5), not 1.0.
        A plain <= rank would create false HIGH signals on flat data.
        """
        hist = [float(x) for x in history if math.isfinite(float(x))]
        if not hist or not math.isfinite(value):
            return 0.5
        less = sum(1 for x in hist if x < value)
        equal = sum(1 for x in hist if x == value)
        rank = (less + 0.5 * equal) / len(hist)
        return max(0.0, min(1.0, rank))

    def _feature_rows(self, rows: list[list[float]]) -> list[dict]:
        out = []
        for i, r in enumerate(rows):
            ts, o, h, l, c, v = r
            if c <= 0:
                continue
            ret3 = 0.0
            if i >= 3 and rows[i - 3][4] > 0:
                ret3 = (c / rows[i - 3][4]) - 1.0
            range_vol = max(0.0, (h - l) / c)
            look = rows[max(0, i - 48):i]
            vols = [float(x[5]) for x in look if float(x[5]) > 0]
            vm, vs = self._mean_std(vols)
            vol_z = ((v - vm) / vs) if vs > 0 else 0.0
            out.append({"ts": int(ts), "ret3": ret3, "range_vol": range_vol, "vol_z": vol_z, "close": c})
        return out

    async def signal_for_symbol(self, symbol: str, settings: dict | None = None) -> RatioSignal | None:
        s = settings or {}
        rows = await self._fetch_closed_1h(symbol, limit=int(float(s.get("ratio_pressure_ohlcv_limit", 200) or 200)))
        min_bars = int(float(s.get("ratio_pressure_min_bars", 90) or 90))
        if len(rows) < min_bars:
            self._last_wait_info[symbol] = {"status": "NOT_ENOUGH_DATA", "bars": len(rows), "min_bars": min_bars}
            log_event("ratio_pressure_not_enough_bars", ok=False, symbol=symbol, bars=len(rows), min_bars=min_bars)
            return None
        feats = self._feature_rows(rows)
        if len(feats) < min_bars:
            self._last_wait_info[symbol] = {"status": "NOT_ENOUGH_DATA", "bars": len(feats), "min_bars": min_bars}
            return None
        cur = feats[-1]
        hist = feats[:-1][-int(float(s.get("ratio_pressure_rank_lookback", 144) or 144)):]
        vol_rank = self._rank(cur["vol_z"], [x["vol_z"] for x in hist])
        range_rank = self._rank(cur["range_vol"], [x["range_vol"] for x in hist])
        ret_rank = self._rank(cur["ret3"], [x["ret3"] for x in hist])
        vol_q = float(s.get("ratio_pressure_vol_q", 0.85) or 0.85)
        range_q = float(s.get("ratio_pressure_range_q", 0.90) or 0.90)
        ret_q = float(s.get("ratio_pressure_ret_q", 0.90) or 0.90)
        ret_low_q = 1.0 - ret_q
        side = ""
        if vol_rank >= vol_q and range_rank >= range_q and ret_rank >= ret_q:
            side = "LONG"
        elif vol_rank >= vol_q and range_rank >= range_q and ret_rank <= ret_low_q:
            side = "SHORT"
        if not side:
            self._last_wait_info[symbol] = {
                "status": "WAIT",
                "vol_rank": vol_rank,
                "range_rank": range_rank,
                "ret3_rank": ret_rank,
                "ret3_pct": cur["ret3"] * 100.0,
                "candle_ts": int(cur["ts"]),
                "need": f"vol>={vol_q:.2f}, range>={range_q:.2f}, LONG ret3>={ret_q:.2f} / SHORT ret3<={ret_low_q:.2f}",
            }
            log_event(
                "ratio_pressure_wait",
                ok=True,
                symbol=symbol,
                vol_rank=round(vol_rank, 4),
                range_rank=round(range_rank, 4),
                ret3_rank=round(ret_rank, 4),
                ret3_pct=round(cur["ret3"] * 100.0, 4),
            )
            return None
        ticker = await self.exchange_client.fetch_ticker(symbol)
        entry = float(ticker.get("last") or ticker.get("close") or cur.get("close") or 0.0)
        if entry <= 0:
            return None
        sl_pct = float(s.get("ratio_pressure_sl_pct", 1.0) or 1.0) / 100.0
        tp_r = float(s.get("ratio_pressure_tp_r", 6.0) or 6.0)
        if side == "LONG":
            stop = entry * (1.0 - sl_pct)
            take = entry * (1.0 + sl_pct * tp_r)
        else:
            stop = entry * (1.0 + sl_pct)
            take = entry * (1.0 - sl_pct * tp_r)
        self._last_wait_info.pop(symbol, None)
        reason = f"vol_rank={vol_rank:.2f} range_rank={range_rank:.2f} ret3_rank={ret_rank:.2f} ret3={cur['ret3']*100:.2f}%"
        log_event(
            "ratio_pressure_signal_found",
            ok=True,
            symbol=symbol,
            side=side,
            price_signal=entry,
            stop_price=stop,
            take_price=take,
            vol_rank=round(vol_rank, 4),
            range_rank=round(range_rank, 4),
            ret3_rank=round(ret_rank, 4),
            ret3_pct=round(cur["ret3"] * 100.0, 4),
            range_pct=round(cur["range_vol"] * 100.0, 4),
            vol_z=round(cur["vol_z"], 4),
            candle_ts=int(cur["ts"]),
            reason=reason,
        )
        return RatioSignal(symbol=symbol, side=side, entry_reference=entry, stop_price=stop, take_price=take, vol_rank=vol_rank, range_rank=range_rank, ret3_rank=ret_rank, ret3_pct=cur["ret3"] * 100.0, range_pct=cur["range_vol"] * 100.0, vol_z=cur["vol_z"], candle_ts=int(cur["ts"]), reason=reason)

    def _balance_total_usdt(self, bal: Any) -> float:
        if not isinstance(bal, dict):
            return 0.0
        for key in ("total", "free"):
            row = bal.get(key)
            if isinstance(row, dict):
                for k in ("USDT", "usdt"):
                    try:
                        v = float(row.get(k) or 0)
                        if v > 0:
                            return v
                    except Exception:
                        pass
        for key in ("USDT", "usdt"):
            row = bal.get(key)
            if isinstance(row, dict):
                for sub in ("total", "free"):
                    try:
                        v = float(row.get(sub) or 0)
                        if v > 0:
                            return v
                    except Exception:
                        pass
        for key in ("total", "equity", "balance"):
            try:
                v = float(bal.get(key) or 0)
                if v > 0:
                    return v
            except Exception:
                pass
        return 0.0

    async def _active_exchange_positions(self) -> list[dict]:
        rows = await self.exchange_client.fetch_positions()
        out = []
        for p in rows or []:
            try:
                qty = self.execution_engine.exchange_position_qty(p)
            except Exception:
                qty = 0.0
            if qty > 0:
                out.append(p)
        return out

    def _row_symbol(self, row: dict) -> str:
        info = row.get("info", {}) if isinstance(row.get("info"), dict) else {}
        raw = str(row.get("symbol") or row.get("mexc_symbol") or info.get("symbol") or "").upper()
        raw = raw.replace("/", "_").replace(":USDT", "")
        if raw.endswith("USDT") and "_" not in raw:
            raw = raw[:-4] + "_USDT"
        return raw

    async def _cooldown_ok(self, symbol: str, side: str, settings: dict) -> tuple[bool, str]:
        cooldown = int(float(settings.get("ratio_pressure_cooldown_sec", 24 * 3600) or (24 * 3600)))
        if cooldown <= 0:
            return True, "ok"
        key = f"ratio_pressure_last_entry_ts_{symbol}_{side}".lower()
        try:
            last = float(await self.storage.get(key, 0) or 0)
        except Exception:
            last = 0.0
        left = cooldown - (time.time() - last)
        if left > 0:
            return False, f"cooldown {int(left/60)}m left"
        return True, "ok"

    async def _mark_entry(self, symbol: str, side: str) -> None:
        key = f"ratio_pressure_last_entry_ts_{symbol}_{side}".lower()
        try:
            await self.storage.set(key, time.time(), bump_revision=False)
        except Exception:
            pass

    async def _build_plan(self, sig: RatioSignal, equity: float, settings: dict) -> TradePlan:
        leverage = int(float(settings.get("ratio_pressure_leverage", 10) or 10))
        margin_pct = 0.10 if sig.symbol.startswith("ETH") else 0.05
        if sig.symbol.startswith("ETH"):
            margin_pct = float(settings.get("ratio_pressure_eth_margin_pct", 10.0) or 10.0) / 100.0
        elif sig.symbol.startswith("BTC"):
            margin_pct = float(settings.get("ratio_pressure_btc_margin_pct", 5.0) or 5.0) / 100.0
        notional = max(0.0, equity * margin_pct * leverage)
        qty = notional / sig.entry_reference if sig.entry_reference > 0 else 0.0
        return TradePlan(
            symbol=sig.symbol,
            side=sig.side,
            order_type="market",
            qty=qty,
            entry_price=sig.entry_reference,
            stop_price=sig.stop_price,
            take_price=sig.take_price,
            risk_pct=0.0,
            confidence=0.90,
            strategy=self.strategy,
            max_open_positions=int(float(settings.get("ratio_pressure_max_active_positions", 2) or 2)),
            planned_notional_usdt=notional,
            expected_margin_usdt=equity * margin_pct,
            max_margin_per_position_usdt=equity * margin_pct,
            leverage=leverage,
            signal_details={
                "mode": "ETH/BTC 1h/1h ratio_pressure_afterimage FINAL_LIVE_CANDIDATE",
                "timeframe": "1h",
                "sl_pct": float(settings.get("ratio_pressure_sl_pct", 1.0) or 1.0),
                "tp_r": float(settings.get("ratio_pressure_tp_r", 6.0) or 6.0),
                "open_type": int(float(settings.get("ratio_pressure_open_type", 1) or 1)),
                "vol_rank": sig.vol_rank,
                "range_rank": sig.range_rank,
                "ret3_rank": sig.ret3_rank,
                "ret3_pct": sig.ret3_pct,
                "range_pct": sig.range_pct,
                "vol_z": sig.vol_z,
                "candle_ts": sig.candle_ts,
                "price_signal": sig.entry_reference,
                "comment": sig.reason,
                "trade_management": {
                    "one_tp_only": True,
                    "breakeven_enabled": False,
                    "trailing_enabled": False,
                    "time_stop_sec": int(float(settings.get("ratio_pressure_time_stop_sec", 8 * 3600) or (8 * 3600))),
                },
            },
        )


    def _fmt_price(self, x: float | int | None) -> str:
        try:
            v = float(x or 0)
            if v >= 1000:
                return f"{v:.2f}"
            if v >= 1:
                return f"{v:.4f}"
            return f"{v:.8f}"
        except Exception:
            return "-"

    def _wait_status_text(self, trigger: str, settings: dict, skipped: list[dict] | None = None) -> str:
        now_s = self._fmt_utc(time.time())
        next_s = self._fmt_utc(self.next_1h_close_ts(delay_sec=int(float(settings.get("ratio_pressure_delay_after_hour_sec", 65) or 65))))
        lines = [
            "🧬 Ratio_pressure 1H",
            "WAIT — сигнала нет",
            f"Scan: {now_s}",
            f"Следующий scan: {next_s}",
            "Новых сделок: 0",
            "",
        ]
        for symbol in self.symbols:
            info = self._last_wait_info.get(symbol) or {}
            if info.get("status") == "NOT_ENOUGH_DATA":
                lines.append(f"{symbol}: WAIT — мало свечей {info.get('bars')}/{info.get('min_bars')}")
            elif info:
                lines.append(
                    f"{symbol}: WAIT | vol={float(info.get('vol_rank', 0)):.2f} "
                    f"range={float(info.get('range_rank', 0)):.2f} "
                    f"ret3={float(info.get('ret3_rank', 0)):.2f} "
                    f"({float(info.get('ret3_pct', 0)):+.2f}%)"
                )
            else:
                lines.append(f"{symbol}: WAIT")
        if skipped:
            lines.append("")
            for x in skipped[:4]:
                lines.append(f"⚠️ {x.get('symbol', '-')}: {str(x.get('reason') or '')[:80]}")
        lines.append("")
        lines.append("Это служебное WAIT-сообщение обновляется/переносится вниз, без спама.")
        return "\n".join(lines)

    def _order_result_text(self, opened: list[dict], skipped: list[dict], trigger: str) -> str:
        lines = ["🧬 Ratio_pressure 1H", f"Trigger: {trigger}"]
        ok_rows = [x for x in opened if x.get("ok")]
        fail_rows = [x for x in opened if not x.get("ok")]
        lines.append(f"Signals: {len(opened) + len(skipped)} | Opened: {len(ok_rows)} | Skipped: {len(skipped)}")
        for x in opened:
            res = x.get("result") if isinstance(x.get("result"), dict) else {}
            ok = bool(x.get("ok"))
            symbol = x.get("symbol") or "-"
            side = x.get("side") or "-"
            if ok:
                tp_ok = bool(res.get("tp_exists") or res.get("take_profit_ok") or res.get("tp_order_id"))
                sl_ok = bool(res.get("sl_exists") or res.get("stop_loss_ok") or res.get("sl_order_id"))
                entry_id = res.get("order_id") or res.get("entry_order_id") or res.get("id")
                if not entry_id and isinstance(res.get("order"), dict):
                    entry_id = (res.get("order") or {}).get("id")
                pos = res.get("position") if isinstance(res.get("position"), dict) else {}
                open_price = pos.get("entry_price") or x.get("entry")
                price_signal = pos.get("planned_entry_price") or ((pos.get("signal_details") or {}).get("price_signal") if isinstance(pos.get("signal_details"), dict) else None) or x.get("entry")
                opened_ts = float(pos.get("opened_at") or 0)
                opened_s = datetime.fromtimestamp(opened_ts, tz=timezone(timedelta(hours=3))).strftime("%H:%M:%S МСК") if opened_ts else "-"
                lines.extend([
                    "",
                    f"✅ {symbol} {side}",
                    f"✅ MARKET entry accepted" + (f" | id={entry_id}" if entry_id else ""),
                    f"✅ Open position: {self._fmt_price(open_price)}",
                    f"✅ SL на бирже: {self._fmt_price(x.get('stop'))}" if sl_ok else f"🚨 SL НЕ подтверждён: {self._fmt_price(x.get('stop'))}",
                    f"✅ TP на бирже: {self._fmt_price(x.get('take'))}" if tp_ok else f"🚨 TP НЕ подтверждён: {self._fmt_price(x.get('take'))}",
                    f"Price signal: {self._fmt_price(price_signal)}",
                    "Protection: " + str(res.get("protection_status") or ("✅ EXCHANGE PROTECTED" if tp_ok and sl_ok else "CHECK REQUIRED")),
                    f"Время открытия позиции: {opened_s}",
                    "Таймер на закрытие: 8:00",
                ])
            else:
                lines.extend(["", f"❌ {symbol} {side} — {str(x.get('reason') or res.get('reason') or '')[:140]}"])
        for x in skipped[:6]:
            lines.append(f"⚠️ {x.get('symbol')} {x.get('side','')} — {str(x.get('reason') or '')[:100]}")
        return "\n".join(lines)

    async def cycle(self, app=None, trigger: str = "schedule") -> dict:
        settings = await self.storage.all_settings()
        live = self._truthy(settings.get("live_trading"), False)
        log_event(
            "ratio_pressure_cycle_start",
            ok=True,
            trigger=trigger,
            live_trading=live,
            next_scan_msk=self._fmt_utc(self.next_1h_close_ts(delay_sec=int(float(settings.get("ratio_pressure_delay_after_hour_sec", 65) or 65)))),
            symbols=",".join(self.symbols),
            tf="1h",
            sl_pct=settings.get("ratio_pressure_sl_pct", 1.0),
            tp_r=settings.get("ratio_pressure_tp_r", 6.0),
            time_stop_sec=settings.get("ratio_pressure_time_stop_sec", 8 * 3600),
            cooldown_sec=settings.get("ratio_pressure_cooldown_sec", 24 * 3600),
        )
        if not live:
            msg = "⛔ Ratio Pressure: Live trading OFF. Сделки не открываю."
            log_event("ratio_pressure_cycle_live_off", ok=False, trigger=trigger)
            await self._notify(msg)
            return {"ok": False, "message": "LIVE_OFF"}

        signals: list[RatioSignal] = []
        skipped: list[dict] = []
        for symbol in self.symbols:
            try:
                sig = await self.signal_for_symbol(symbol, settings)
                if sig:
                    signals.append(sig)
            except Exception as e:
                skipped.append({"symbol": symbol, "ok": False, "reason": f"signal error: {e}"})
                log_event("ratio_pressure_signal_error", ok=False, symbol=symbol, error=str(e)[:700])

        if not signals:
            log_event("ratio_pressure_cycle_wait", ok=True, trigger=trigger)
            await self._notify_status(self._wait_status_text(trigger, settings, skipped))
            return {"ok": True, "message": "WAIT", "opened": [], "skipped": skipped}

        try:
            ex_positions = await self._active_exchange_positions()
        except Exception as e:
            reason = f"cannot verify exchange positions: {e}"
            log_event("ratio_pressure_position_verify_failed", ok=False, error=str(e)[:700])
            await self._notify("❌ Ratio Pressure: " + reason)
            return {"ok": False, "message": reason, "opened": [], "skipped": skipped}

        max_active = int(float(settings.get("ratio_pressure_max_active_positions", 2) or 2))
        active_symbols = {self._row_symbol(p) for p in ex_positions if self._row_symbol(p)}
        opened: list[dict] = []

        def _skip(sig_obj: RatioSignal, reason: str) -> None:
            row = {"symbol": sig_obj.symbol, "side": sig_obj.side, "ok": False, "reason": reason}
            skipped.append(row)
            log_event("ratio_pressure_signal_skipped", ok=False, symbol=sig_obj.symbol, side=sig_obj.side, reason=reason)

        balance = await self.exchange_client.fetch_balance()
        equity = self._balance_total_usdt(balance)
        if equity <= 0:
            equity = float(os.getenv("DEFAULT_EQUITY_USDT", "100") or 100)
        log_event(
            "ratio_pressure_balance_snapshot",
            ok=True,
            equity_usdt=equity,
            max_active_positions=max_active,
            exchange_active_symbols=",".join(sorted(active_symbols)),
            exchange_positions_count=len(ex_positions),
        )

        # ETH first: it is the primary module from research. BTC is half-size add-on.
        signals.sort(key=lambda x: 0 if x.symbol.startswith("ETH") else 1)
        for sig in signals:
            if len(ex_positions) + len([x for x in opened if x.get("ok")]) >= max_active:
                _skip(sig, f"max active positions {max_active}")
                continue
            if sig.symbol in active_symbols:
                _skip(sig, "existing exchange position on symbol")
                continue
            cd_ok, cd_reason = await self._cooldown_ok(sig.symbol, sig.side, settings)
            if not cd_ok:
                _skip(sig, cd_reason)
                continue
            plan = await self._build_plan(sig, equity, settings)
            if plan.qty <= 0:
                _skip(sig, "qty<=0")
                continue
            log_event(
                "ratio_pressure_plan_built",
                ok=True,
                symbol=sig.symbol,
                side=sig.side,
                qty=plan.qty,
                equity_usdt=equity,
                margin_usdt=plan.expected_margin_usdt,
                notional_usdt=plan.planned_notional_usdt,
                leverage=plan.leverage,
                order_type=plan.order_type,
                price_signal=sig.entry_reference,
                stop_price=sig.stop_price,
                take_price=sig.take_price,
                reason=sig.reason,
            )
            log_event(
                "ratio_pressure_place_start",
                ok=True,
                symbol=sig.symbol,
                side=sig.side,
                entry=sig.entry_reference,
                stop=sig.stop_price,
                take=sig.take_price,
                qty=plan.qty,
                notional=plan.planned_notional_usdt,
                reason=sig.reason,
            )
            res = await self.execution_engine.place_entry(plan, live=True)
            ok = bool((res or {}).get("ok"))
            row = {"symbol": sig.symbol, "side": sig.side, "ok": ok, "entry": sig.entry_reference, "stop": sig.stop_price, "take": sig.take_price, "reason": (res or {}).get("reason", ""), "result": res}
            log_event(
                "ratio_pressure_place_result",
                ok=ok,
                symbol=sig.symbol,
                side=sig.side,
                price_signal=sig.entry_reference,
                stop_price=sig.stop_price,
                take_price=sig.take_price,
                result=res,
                tp_confirmed=bool((res or {}).get("tp_exists") or (res or {}).get("take_profit_ok") or (res or {}).get("tp_order_id")),
                sl_confirmed=bool((res or {}).get("sl_exists") or (res or {}).get("stop_loss_ok") or (res or {}).get("sl_order_id")),
            )
            opened.append(row)
            if ok:
                await self._mark_entry(sig.symbol, sig.side)
                active_symbols.add(sig.symbol)
            await asyncio.sleep(float(settings.get("ratio_pressure_sequence_delay_sec", 1.2) or 1.2))

        placed = [x for x in opened if x.get("ok")]
        if placed or opened or skipped:
            await self._notify(self._order_result_text(opened, skipped, trigger))
        log_event("ratio_pressure_cycle_done", ok=True, trigger=trigger, signals=len(signals), placed=len(placed), skipped=skipped, opened=opened)
        return {"ok": True, "message": "DONE", "opened": opened, "skipped": skipped}
