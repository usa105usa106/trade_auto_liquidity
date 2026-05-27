from __future__ import annotations

import asyncio, base64, json, os, time, math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import numpy as np
import pandas as pd

from models import TradePlan
from openai_signal_engine import openai_key, active_model
from chart_renderer import render_trade_setup_chart
from debug_log import log_event

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

@dataclass
class BTCAutopilotDecision:
    signal: str = "WAIT"
    probability: float = 0.0
    grade: str = "C"
    entry_zone_low: float = 0.0
    entry_zone_high: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float = 0.0
    take_profit_3: float = 0.0
    reason: str = ""
    raw: str = ""
    error: str = ""

class BTCVisionAutopilot:
    symbol = "BTC_USDT"

    def __init__(self, storage, exchange_client, execution_engine, app_notify=None):
        self.storage = storage
        self.exchange_client = exchange_client
        self.execution_engine = execution_engine
        self.app_notify = app_notify
        self._running = False

    @staticmethod
    def next_msk_close_ts(now: float | None = None) -> float:
        now_dt = datetime.fromtimestamp(now or time.time(), tz=timezone.utc) + timedelta(hours=3)
        slots = [3,7,11,15,19,23]
        for h in slots:
            d = now_dt.replace(hour=h, minute=0, second=0, microsecond=0)
            if d.timestamp() > now_dt.timestamp() + 5:
                return (d - timedelta(hours=3)).timestamp()
        d = (now_dt + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
        return (d - timedelta(hours=3)).timestamp()

    async def run_loop(self, app):
        self._running = True
        while self._running:
            settings = await self.storage.all_settings()
            # Virtual management must keep working even when BTC AI entry mode is OFF.
            # OFF disables only new analysis/new entries; it must not abandon TP1 -> breakeven handling.
            try:
                await self.cancel_stale_pending()
                await self.monitor_tp1_breakeven(app)
                await self.monitor_24h_time_exit(app)
                if self._bool(settings, "btc_ai_autopilot_enabled", False):
                    await self._apply_stop_loss_pause_if_needed(app)
            except Exception:
                pass
            if not self._bool(await self.storage.all_settings(), "btc_ai_autopilot_enabled", False):
                await asyncio.sleep(10)
                continue
            target = self.next_msk_close_ts()
            await self._notify(app, f"🤖 BTC AI автопилот ON. Следующий анализ после закрытия 4H свечи: {self._fmt_msk(target)}")
            while time.time() < target:
                try:
                    await self.cancel_stale_pending()
                    await self.monitor_tp1_breakeven(app)
                    await self.monitor_24h_time_exit(app)
                    await self._apply_stop_loss_pause_if_needed(app)
                    # Pending entries are also replaced on the next closed 4H candle when there is no active position.
                except Exception:
                    pass
                if not self._bool(await self.storage.all_settings(), "btc_ai_autopilot_enabled", False):
                    break
                await asyncio.sleep(min(30, max(1, target - time.time())))
            if not self._bool(await self.storage.all_settings(), "btc_ai_autopilot_enabled", False):
                continue
            try:
                await self.cycle(app)
            except Exception as e:
                log_event("btc_ai_cycle_error", ok=False, error=str(e)[:1200])
                await self._notify(app, f"❌ BTC AI cycle error: {str(e)[:800]}. Сделка не открывается.")
            await asyncio.sleep(5)

    def stop(self):
        self._running = False

    async def _btc_ai_positions(self) -> list[dict]:
        try:
            positions = await self.storage.positions()
        except Exception:
            return []
        out = []
        for p in positions:
            if str(p.get("strategy") or "") != "btc_ai_4h":
                continue
            if str(p.get("symbol") or "").upper() not in {"BTC_USDT", "BTCUSDT", "BTC/USDT"}:
                continue
            out.append(p)
        return out

    async def active_btc_position(self) -> dict | None:
        for p in await self._btc_ai_positions():
            if str(p.get("status") or "").lower() == "open":
                return p
        return None

    async def pending_btc_entries(self) -> list[dict]:
        return [p for p in await self._btc_ai_positions() if str(p.get("status") or "").lower() == "pending"]

    async def cancel_pending_btc_entries(self, app, reason: str = "btc_ai_4h_new_candle_replace") -> int:
        pending = await self.pending_btc_entries()
        n = 0
        for p in pending:
            try:
                await self.execution_engine.cancel_entry(p, live=True, reason=reason)
                n += 1
            except Exception as e:
                await self._notify(app, f"⚠️ BTC AI: не смог отменить старую лимитку {p.get('order_id')}: {str(e)[:250]}")
        if n:
            await self._notify(app, f"♻️ BTC AI: старая лимитка без активной сделки отменена. Готовлю новый 4H сигнал.")
        return n

    async def cycle(self, app, force_live_test: bool = False):
        settings = await self.storage.all_settings()
        if not force_live_test and not self._bool(settings, "btc_ai_autopilot_enabled", False):
            return
        await self._hard_disable_other_modes(settings)
        if await self._apply_stop_loss_pause_if_needed(app):
            return
        symbol = str(settings.get("btc_ai_symbol", self.symbol) or self.symbol)

        active = await self.active_btc_position()
        if active:
            await self._notify(app, "⚠️ BTC AI 4H\n\nНовая 4H свеча закрылась.\nНо уже есть активная BTC позиция.\n\nНовый вход пропущен.\nТекущая сделка продолжает виртуальное сопровождение.")
            return

        # If the previous 4H limit was not filled, replace it with a fresh signal.
        await self.cancel_pending_btc_entries(app, reason="btc_ai_4h_new_candle_replace")

        candles = await self.exchange_client.fetch_ohlcv(symbol, timeframe="4h", limit=160)
        if len(candles) < 80:
            await self._notify(app, "⚠️ BTC AI: мало свечей MEXC для анализа")
            return
        market_data = await self.collect_market_data(symbol, candles)
        fatal = self._market_data_fatal_error(market_data)
        if fatal:
            log_event("btc_ai_market_data_error", ok=False, error=fatal, market_data=market_data)
            await self._notify(app, f"❌ BTC AI: ошибка данных {fatal}. Сделка не открывается.")
            return
        chart_path = await asyncio.to_thread(self.render_chart, symbol, candles, market_data)
        decision = await self.ask_ai(settings, chart_path, market_data)
        plan_levels = self.prepare_levels(decision, market_data) if not decision.error else {}
        annotated_path = await asyncio.to_thread(self.render_signal_chart, symbol, candles, market_data, decision, plan_levels) if plan_levels else chart_path
        caption = self.format_decision(decision, market_data, plan_levels)
        try:
            with open(annotated_path, "rb") as img:
                await app.bot.send_photo(chat_id=self._admin_id(), photo=img, caption=caption[:1024])
        except Exception:
            await self._notify(app, caption)
        if decision.error:
            if not force_live_test:
                return
            # LIVE TEST OVERRIDE: even if OpenAI failed, verify MEXC order mechanics with a tiny controlled BTC market trade.
            last_price = float(market_data.get("last_price") or 0)
            decision = BTCAutopilotDecision(
                signal="LONG",
                probability=0.0,
                grade="TEST",
                entry_zone_low=last_price * 0.999 if last_price else 0,
                entry_zone_high=last_price * 1.001 if last_price else 0,
                stop_loss=last_price * 0.99 if last_price else 0,
                reason=f"LIVE TEST OVERRIDE after AI error: {decision.error}",
                error="",
            )
            plan_levels = self.prepare_levels(decision, market_data, forced_entry=last_price)
            log_event("btc_ai_live_test_override", ok=True, reason="ai_error", decision=decision.__dict__, plan_levels=plan_levels)
        prob = float(decision.probability or 0)
        if force_live_test:
            last_price = float(market_data.get("last_price") or 0)
            if decision.signal not in {"LONG", "SHORT"}:
                decision.signal = "LONG"
                decision.reason = (decision.reason + " | " if decision.reason else "") + "LIVE TEST OVERRIDE: AI signal was not tradable, forced LONG market to test MEXC mechanics."
            if float(decision.entry_zone_low or 0) <= 0 or float(decision.entry_zone_high or 0) <= 0:
                decision.entry_zone_low = last_price * 0.999 if last_price else 0
                decision.entry_zone_high = last_price * 1.001 if last_price else 0
            if float(decision.stop_loss or 0) <= 0 and last_price:
                decision.stop_loss = last_price * (0.99 if decision.signal == "LONG" else 1.01)
            decision.grade = str(decision.grade or "TEST") + " LIVE_TEST"
            plan_levels = self.prepare_levels(decision, market_data, forced_entry=last_price)
            log_event("btc_ai_live_test_force_trade", ok=True, original_probability=prob, forced_signal=decision.signal, decision=decision.__dict__, plan_levels=plan_levels)
        elif decision.signal not in {"LONG", "SHORT"} or prob < float(settings.get("btc_ai_min_trade_probability", 75) or 75):
            await self._notify(app, f"🟡 BTC AI: сделки нет. Signal={decision.signal}, probability={prob:.1f}%")
            return
        # Race-safety: if a pending order got filled while AI was thinking, do not stack another BTC trade.
        active = await self.active_btc_position()
        if active:
            await self._notify(app, "⚠️ BTC AI 4H\n\nНовый анализ получен.\nНо уже есть активная BTC позиция.\n\nНовый сигнал пропущен.\nТекущая сделка продолжает сопровождение.")
            return
        # Race-safety: if an old pending is still present, cancel it before placing the new limit/market.
        await self.cancel_pending_btc_entries(app, reason="btc_ai_4h_new_signal_replace")
        await self.execute_decision(app, settings, symbol, decision, market_data, plan_levels, force_market=force_live_test)

    async def execute_decision(self, app, settings, symbol, d: BTCAutopilotDecision, market_data: dict, plan_levels: dict | None = None, force_market: bool = False):
        live = self._bool(settings, "live_trading", False)
        price = float(market_data.get("last_price") or 0)
        balance = await self.exchange_client.fetch_balance()
        total = float(((balance.get("USDT") or {}).get("total") or (balance.get("total") or {}).get("USDT") or 0) or 0)
        if total <= 0 or price <= 0:
            await self._notify(app, "❌ BTC AI: не смог получить баланс или цену")
            return
        balance_share = float(settings.get("btc_ai_balance_share", 0.10) or 0.10)
        leverage = int(float(settings.get("btc_ai_leverage", 10) or 10))
        margin = total * balance_share
        notional = margin * leverage
        qty_total = notional / price
        lv = plan_levels or self.prepare_levels(d, market_data, forced_entry=price if d.probability >= 85 else None)
        entry_low, entry_high = float(lv.get("entry_low") or 0), float(lv.get("entry_high") or 0)
        entry_mid = float(lv.get("entry_mid") or 0)
        stop = float(lv.get("stop_loss") or 0)
        tp1 = float(lv.get("take_profit_1") or 0)
        tp2 = float(lv.get("take_profit_2") or 0)
        if tp2 <= 0 or tp1 <= 0 or stop <= 0 or entry_mid <= 0:
            await self._notify(app, "❌ BTC AI: неполные уровни SL/TP/entry после risk-check")
            return
        maxpos = 1
        common = {"btc_ai": True, "probability": d.probability, "entry_zone": [entry_low, entry_high], "cancel_after_sec": 14400, "reason": d.reason, "tp1_percent": 2.0, "tp2_percent": 4.0, "tp1_fraction": 0.50, "move_sl_to_be_after_tp1": True}
        if force_market or d.probability >= 85:
            plan_m = TradePlan(symbol=symbol, side=d.signal, order_type="market", qty=qty_total, entry_price=price, stop_price=stop, take_price=tp2, partial_take_price=tp1, partial_take_fraction=0.50, final_take_price=tp2, risk_pct=0.0, confidence=d.probability/100, strategy="btc_ai_4h", max_open_positions=maxpos, planned_notional_usdt=notional, expected_margin_usdt=margin, leverage=leverage, signal_details=common)
            log_event("btc_ai_order_request", symbol=symbol, side=d.signal, order_type="market", force_live_test=force_market, qty=qty_total, entry=price, stop=stop, tp1=tp1, tp2=tp2, probability=d.probability, margin=margin, leverage=leverage, venue="MEXC")
            res_m = await self.execution_engine.place_entry(plan_m, live=live)
            log_event("btc_ai_order_response", symbol=symbol, side=d.signal, order_type="market", response=res_m, ok=bool(isinstance(res_m, dict) and res_m.get("ok", True)))
            
            status_ok = bool(isinstance(res_m, dict) and res_m.get("ok", True))
            protection = "EXCHANGE PROTECTED" if status_ok else "ПРОВЕРЬ /log"
            await self._notify(app, f"✅ BTC AI {'LIVE TEST' if force_market else 'A+'} MARKET 100%\n"
                                    f"Вход: ~{price:.2f}\n"
                                    f"SL: {stop:.2f}\n"
                                    f"TP1: {tp1:.2f} (50%)\n"
                                    f"TP2: {tp2:.2f} (остаток)\n"
                                    f"Проходимость: {d.probability:.1f}%\n"
                                    f"Защита: {protection}\n"
                                    f"Виртуальное сопровождение: ВКЛ")
            return
        plan = TradePlan(symbol=symbol, side=d.signal, order_type="limit", qty=qty_total, entry_price=entry_mid, stop_price=stop, take_price=tp2, partial_take_price=tp1, partial_take_fraction=0.50, final_take_price=tp2, risk_pct=0.0, confidence=d.probability/100, strategy="btc_ai_4h", max_open_positions=maxpos, planned_notional_usdt=notional, expected_margin_usdt=margin, leverage=leverage, signal_details=common)
        log_event("btc_ai_order_request", symbol=symbol, side=d.signal, order_type="limit", qty=qty_total, entry=entry_mid, entry_zone=[entry_low, entry_high], stop=stop, tp1=tp1, tp2=tp2, probability=d.probability, margin=margin, leverage=leverage, venue="MEXC")
        res = await self.execution_engine.place_entry(plan, live=live)
        log_event("btc_ai_order_response", symbol=symbol, side=d.signal, order_type="limit", response=res, ok=bool(isinstance(res, dict) and res.get("ok", True)))
        
        await self._notify(app, f"✅ BTC AI LIMIT выставлен\n"
                                f"Вход: {entry_mid:.2f}\n"
                                f"SL: {stop:.2f}\n"
                                f"TP1: {tp1:.2f} (50%)\n"
                                f"TP2: {tp2:.2f} (остаток)\n"
                                f"Проходимость: {d.probability:.1f}%\n"
                                f"Лимитка живет: 4 часа\n"
                                f"Виртуальное сопровождение: ВКЛ")

    def prepare_levels(self, d: BTCAutopilotDecision, market_data: dict, forced_entry: float | None = None) -> dict:
        if d.signal not in {"LONG", "SHORT"}:
            return {}
        try:
            entry_low, entry_high = sorted([float(d.entry_zone_low), float(d.entry_zone_high)])
            entry_mid = float(forced_entry or ((entry_low + entry_high) / 2.0))
            if entry_mid <= 0:
                entry_mid = float(market_data.get("last_price") or 0)
            if entry_mid <= 0:
                return {}
            side = d.signal.upper()
            # Stop may be proposed by AI, but bot always corrects it into the 1%-2% range.
            # If AI gives <1%, use 1%; if >2%, use 2%; if missing/invalid, use safe default 1.5%.
            ai_stop = float(d.stop_loss or 0)
            stop_pct = 1.5
            if ai_stop > 0:
                pct = abs(entry_mid - ai_stop) / entry_mid * 100.0
                if pct < 1.0:
                    stop_pct = 1.0
                elif pct > 2.0:
                    stop_pct = 2.0
                else:
                    stop_pct = pct
            if side == "LONG":
                stop = entry_mid * (1 - stop_pct / 100.0)
                tp1 = entry_mid * 1.02
                tp2 = entry_mid * 1.04
            else:
                stop = entry_mid * (1 + stop_pct / 100.0)
                tp1 = entry_mid * 0.98
                tp2 = entry_mid * 0.96
            return {"entry_low": entry_low, "entry_high": entry_high, "entry_mid": entry_mid, "stop_loss": stop, "take_profit_1": tp1, "take_profit_2": tp2, "stop_pct": stop_pct, "tp1_pct": 2.0, "tp2_pct": 4.0}
        except Exception:
            return {}

    def _prepare_chart_df(self, candles: list, tail: int = 90) -> pd.DataFrame:
        df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
        df["dt"] = pd.to_datetime(df.ts, unit="ms")
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = df[c].astype(float)
        df = df.tail(tail).reset_index(drop=True)
        df["MA7"] = df.close.rolling(7).mean()
        df["MA25"] = df.close.rolling(25).mean()
        df["MA99"] = df.close.rolling(99).mean()
        ema12 = df.close.ewm(span=12, adjust=False).mean()
        ema26 = df.close.ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["Signal"] = df.MACD.ewm(span=9, adjust=False).mean()
        df["Hist"] = df.MACD - df.Signal
        return df

    def _draw_clean_btc_chart(self, df: pd.DataFrame, market_data: dict, levels: dict | None = None, decision: BTCAutopilotDecision | None = None, filename_prefix: str = "btc_ai_clean") -> str:
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle

        bg = "#0f1722"
        grid = "#263241"
        txt = "#d5dde8"
        green = "#21c087"
        red = "#f6465d"
        orange = "#f59e0b"
        blue = "#3b82f6"
        purple = "#a855f7"

        fig = plt.figure(figsize=(12.8, 7.2), dpi=100)
        gs = fig.add_gridspec(3, 1, height_ratios=[5.2, 1.2, 1.5], hspace=0.06)
        ax = fig.add_subplot(gs[0])
        av = fig.add_subplot(gs[1], sharex=ax)
        am = fig.add_subplot(gs[2], sharex=ax)
        fig.patch.set_facecolor(bg)
        for a in (ax, av, am):
            a.set_facecolor(bg)
            a.grid(True, color=grid, alpha=0.42, linewidth=0.8)
            a.tick_params(colors=txt, labelsize=9)
            a.yaxis.tick_right()
            for sp in a.spines.values():
                sp.set_color(grid)

        x = np.arange(len(df))
        w = 0.58
        for i, r in enumerate(df.itertuples()):
            col = green if r.close >= r.open else red
            ax.vlines(i, r.low, r.high, color=col, linewidth=1.05, alpha=0.95)
            body_low = min(r.open, r.close)
            body_h = max(abs(r.close - r.open), max(df.close.iloc[-1] * 0.00005, 1.0))
            ax.add_patch(Rectangle((i - w / 2, body_low), w, body_h, facecolor=col, edgecolor=col, linewidth=0.6))

        ax.plot(x, df.MA7, color=blue, linewidth=1.25, label=f"MA7 {df.MA7.iloc[-1]:.1f}")
        ax.plot(x, df.MA25, color=orange, linewidth=1.25, label=f"MA25 {df.MA25.iloc[-1]:.1f}")
        if not np.isnan(df.MA99.iloc[-1]):
            ax.plot(x, df.MA99, color=purple, linewidth=1.35, label=f"MA99 {df.MA99.iloc[-1]:.1f}")

        last = float(market_data.get("last_price") or df.close.iloc[-1])
        all_price_levels = [float(df.low.min()), float(df.high.max()), last]
        if levels:
            all_price_levels += [float(levels.get(k) or 0) for k in ["entry_mid", "stop_loss", "take_profit_1", "take_profit_2"]]
        all_price_levels = [v for v in all_price_levels if v > 0]
        ymin, ymax = min(all_price_levels), max(all_price_levels)
        pad = max((ymax - ymin) * 0.12, last * 0.003)
        ax.set_ylim(ymin - pad, ymax + pad)
        ax.set_xlim(-1, len(df) + 12)

        # Current price line
        ax.axhline(last, color=txt, linestyle=":", linewidth=1.1, alpha=0.75)
        ax.text(len(df) + 0.4, last, f"LAST {last:.1f}", color=txt, va="center", fontsize=9,
                bbox=dict(boxstyle="round,pad=0.25", facecolor="#111827", edgecolor=txt, alpha=0.75))

        if levels:
            entry = float(levels.get("entry_mid") or 0)
            sl = float(levels.get("stop_loss") or 0)
            tp1 = float(levels.get("take_profit_1") or 0)
            tp2 = float(levels.get("take_profit_2") or 0)
            e_low = float(levels.get("entry_low") or entry)
            e_high = float(levels.get("entry_high") or entry)
            span_left = 0.58
            if entry > 0 and sl > 0:
                ax.axhspan(min(entry, sl), max(entry, sl), xmin=span_left, xmax=1.0, color=red, alpha=0.12)
            if entry > 0 and tp2 > 0:
                ax.axhspan(min(entry, tp2), max(entry, tp2), xmin=span_left, xmax=1.0, color=green, alpha=0.10)
            if e_low > 0 and e_high > 0:
                ax.axhspan(min(e_low, e_high), max(e_low, e_high), xmin=span_left, xmax=1.0, color=orange, alpha=0.18)
            level_rows = [(entry, "ENTRY", orange), (sl, "SL", red), (tp1, "TP1 +2%", green), (tp2, "TP2 +4%", green)]
            for val, label, col in level_rows:
                if val <= 0:
                    continue
                ax.axhline(val, color=col, linestyle="--", linewidth=1.15, alpha=0.95)
                ax.text(len(df) + 0.4, val, f"{label} {val:.1f}", color=col, va="center", fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.22", facecolor="#111827", edgecolor=col, alpha=0.78))

        title_sig = ""
        if decision:
            title_sig = f" · {decision.signal} {decision.probability:.1f}%"
        ax.set_title(f"BTC_USDT · MEXC Futures · 4H{title_sig} · Last {last:.1f}",
                     color=txt, loc="left", fontsize=13, fontweight="bold")
        ax.legend(loc="upper left", frameon=False, labelcolor=txt, fontsize=8)

        cols = [green if c >= o else red for o, c in zip(df.open, df.close)]
        av.bar(x, df.volume, color=cols, alpha=0.62, width=w)
        av.text(0, max(df.volume.max() * 0.78, 1), f"MEXC Volume ratio {float(market_data.get('mexc_volume_ratio_30') or 0):.2f}x", color=txt, fontsize=8)

        hcols = [green if h >= 0 else red for h in df.Hist]
        am.bar(x, df.Hist, color=hcols, alpha=0.72, width=w)
        am.plot(x, df.MACD, color=blue, linewidth=1.15, label="MACD")
        am.plot(x, df.Signal, color=orange, linewidth=1.15, label="Signal")
        am.axhline(0, color=txt, alpha=0.45, linewidth=0.8)
        am.legend(loc="upper left", frameon=False, labelcolor=txt, fontsize=8)

        step = max(10, len(df) // 6)
        ticks = list(range(0, len(df), step))
        am.set_xticks(ticks)
        am.set_xticklabels([df.dt.iloc[i].strftime("%m-%d %H:%M") for i in ticks], color=txt)
        plt.setp(ax.get_xticklabels(), visible=False)
        plt.setp(av.get_xticklabels(), visible=False)

        out = Path("/tmp") / f"{filename_prefix}_{int(time.time())}.jpg"
        fig.savefig(out, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.08, dpi=100)
        plt.close(fig)
        return str(out)

    def render_signal_chart(self, symbol: str, candles: list, market_data: dict, d: BTCAutopilotDecision, lv: dict) -> str:
        df = self._prepare_chart_df(candles, tail=90)
        return self._draw_clean_btc_chart(df, market_data, levels=lv, decision=d, filename_prefix="btc_ai_signal_clean")

    async def monitor_tp1_breakeven(self, app):
        """After TP1 is no longer active and price has touched TP1, move SL to breakeven for the remaining BTC position."""
        try:
            positions = await self.storage.positions()
            for pos in positions:
                if pos.get("status") != "open" or str(pos.get("strategy")) != "btc_ai_4h":
                    continue
                if pos.get("btc_ai_tp1_be_done"):
                    continue
                tp1 = float(pos.get("partial_take_price") or 0); entry = float(pos.get("entry_price") or 0)
                if tp1 <= 0 or entry <= 0:
                    continue
                side = str(pos.get("side") or "").upper()
                ticker = await self.exchange_client.fetch_ticker(pos.get("symbol") or self.symbol)
                price = float(ticker.get("last") or 0)
                touched = (side == "LONG" and price >= tp1) or (side == "SHORT" and price <= tp1)
                if not touched:
                    continue
                # If TP1 trigger is still active, wait; if endpoint unavailable, price touch is used as virtual confirmation.
                tp1_id = str(pos.get("tp1_order_id") or "")
                if tp1_id and hasattr(self.exchange_client, "mexc_find_active_plan_order"):
                    row = await self.exchange_client.mexc_find_active_plan_order(pos.get("symbol") or self.symbol, order_id=tp1_id)
                    if row:
                        continue
                # Cancel all old protective orders and attach fresh TP2 + SL at entry for the remainder.
                try:
                    await self.exchange_client.cancel_all_orders(pos.get("symbol") or self.symbol)
                except Exception:
                    pass
                remaining_qty = max(0.0, float(pos.get("qty") or 0) * 0.50)
                close_side = "sell" if side == "LONG" else "buy"
                tp2 = float(pos.get("take_price") or pos.get("final_take_price") or 0)
                if remaining_qty > 0 and tp2 > 0:
                    try: await self.execution_engine._create_take_profit_market_order(pos["symbol"], close_side, remaining_qty, tp2)
                    except Exception: pass
                    try: await self.execution_engine._create_stop_market_order(pos["symbol"], close_side, remaining_qty, entry)
                    except Exception: pass
                pos["qty"] = remaining_qty or pos.get("qty")
                pos["stop_price"] = entry
                pos["breakeven_moved"] = True
                pos["btc_ai_tp1_be_done"] = True
                pos["updated_at"] = time.time()
                await self.storage.upsert_position(pos)
                await self._notify(app, f"🟢 BTC AI TP1 взят. Стоп перенесен в Б/У: {entry:.2f}. TP2 остается: {tp2:.2f}")
        except Exception as e:
            await self._notify(app, f"⚠️ BTC AI BE monitor warning: {str(e)[:300]}")

    async def monitor_24h_time_exit(self, app):
        """BTC AI time-stop: after 24h, do not force-close a loser.

        If the trade is older than 24h and still not fully completed, close it
        only when current price is breakeven or better. If it is negative, keep
        virtual management on and close as soon as it returns to breakeven.
        """
        try:
            positions = await self.storage.positions()
            for pos in positions:
                if pos.get("status") != "open" or str(pos.get("strategy")) != "btc_ai_4h":
                    continue
                if pos.get("btc_ai_24h_exit_done"):
                    continue
                opened_at = float(pos.get("opened_at") or pos.get("created_at") or 0)
                if opened_at <= 0 or time.time() - opened_at < 86400:
                    continue
                entry = float(pos.get("entry_price") or 0)
                if entry <= 0:
                    continue
                side = str(pos.get("side") or "").upper()
                ticker = await self.exchange_client.fetch_ticker(pos.get("symbol") or self.symbol)
                price = float(ticker.get("last") or 0)
                if price <= 0:
                    continue
                pnl_ok = (side == "LONG" and price >= entry) or (side == "SHORT" and price <= entry)
                if not pnl_ok:
                    if not pos.get("btc_ai_24h_wait_be_notified"):
                        pos["btc_ai_24h_wait_be_notified"] = True
                        pos["btc_ai_24h_wait_be_started_at"] = time.time()
                        await self.storage.upsert_position(pos)
                        await self._notify(app, f"⏳ BTC AI 24H: сделка открыта больше суток, но сейчас в минусе. Не закрываю. Закрою автоматически, когда цена вернется в Б/У: {entry:.2f}")
                    continue
                live = self._bool(await self.storage.all_settings(), "live_trading", False)
                # Cancel protective orders before market close to avoid stale reduce-only triggers.
                try:
                    await self.exchange_client.cancel_all_orders(pos.get("symbol") or self.symbol)
                except Exception:
                    pass
                res = await self.execution_engine.close_position(pos, reason="btc_ai_24h_breakeven_time_exit", live=live, exit_price=price)
                pos["btc_ai_24h_exit_done"] = True
                pos["updated_at"] = time.time()
                try:
                    await self.storage.upsert_position(pos)
                except Exception:
                    pass
                await self._notify(app, f"⏰ BTC AI 24H: сделка открыта больше суток и вышла в Б/У/плюс. Закрыл по рынку. Entry {entry:.2f}, current {price:.2f}. Детали в /log")
        except Exception as e:
            await self._notify(app, f"⚠️ BTC AI 24H monitor warning: {str(e)[:300]}")

    async def cancel_stale_pending(self):
        try:
            positions = await self.storage.positions()
            for p in positions:
                if p.get("status") != "pending":
                    continue
                if str(p.get("strategy")) != "btc_ai_4h":
                    continue
                if time.time() - float(p.get("opened_at") or 0) >= 14400:
                    await self.execution_engine.cancel_entry(p, live=True, reason="btc_ai_4h_limit_timeout")
        except Exception:
            pass

    async def collect_market_data(self, symbol: str, candles: list) -> dict:
        ticker = await self.exchange_client.fetch_ticker(symbol)
        depth = await self.exchange_client.fetch_order_book(symbol, limit=50)
        funding = await self._mexc_funding(symbol)
        spot = await self._binance_spot_pressure("BTCUSDT")
        liq = await self._mexc_liquidation_proxy(symbol)
        df = pd.DataFrame(candles, columns=["ts","open","high","low","close","volume"])
        for c in ["open","high","low","close","volume"]: df[c] = df[c].astype(float)
        last = float(df.close.iloc[-1])
        vol_ratio = float(df.volume.iloc[-1] / max(1e-9, df.volume.tail(30).mean()))
        spot_norm = self._normalize_cross_exchange_pressure(spot, vol_ratio)
        return {
            "symbol": symbol,
            "timeframe": "4h",
            "execution_venue": "MEXC futures",
            "chart_source": "MEXC futures",
            "volume_source": "MEXC futures",
            "funding_source": "MEXC futures",
            "liquidation_source": "MEXC futures/proxy",
            "spot_confirmation_source": "Binance spot",
            "normalization_note": "Do NOT compare raw MEXC futures volume USDT to raw Binance spot volume USDT. MEXC futures liquidity is lower. Use each venue only in its own context: MEXC for executable futures structure/volume/funding/orderbook, Binance spot only as directional confirmation using buy_ratio/delta_score, not absolute size.",
            "last_price": float(ticker.get("last") or last),
            "mexc_volume_last": float(df.volume.iloc[-1]),
            "mexc_volume_ratio_30": vol_ratio,
            "funding": funding,
            "mexc_orderbook": self._book_summary(depth),
            "binance_spot_pressure": spot,
            "cross_exchange_pressure_normalized": spot_norm,
            "mexc_liquidations_proxy": liq,
            "closed_candle_msk": self._fmt_msk(df.ts.iloc[-1]/1000),
            "candles_count": len(candles)
        }

    def _normalize_cross_exchange_pressure(self, spot: dict, mexc_volume_ratio_30: float) -> dict:
        """Normalize Binance spot confirmation so AI does not compare raw venue volumes.

        Binance spot BTC volume can be many times bigger than MEXC futures volume.
        For the BTC AI mode the bot treats Binance spot only as a directional
        confirmation layer. Raw Binance notional is deliberately converted into
        ratio/score fields before it reaches the prompt.
        """
        try:
            buy_ratio = float((spot or {}).get("buy_ratio") or 0.5)
            delta = float((spot or {}).get("delta_usdt") or 0.0)
            total = float((spot or {}).get("buy_usdt") or 0.0) + float((spot or {}).get("sell_usdt") or 0.0)
            # Direction score from -1 to +1, based on spot aggression only.
            delta_score = (delta / total) if total > 0 else 0.0
            # Confidence of spot confirmation, not raw size.
            # 0.50 buy_ratio = neutral, 0.60+ = solid buy pressure, 0.40- = solid sell pressure.
            if buy_ratio >= 0.58:
                direction = "BUY_CONFIRMATION"
            elif buy_ratio <= 0.42:
                direction = "SELL_CONFIRMATION"
            else:
                direction = "NEUTRAL"
            return {
                "binance_spot_direction": direction,
                "binance_spot_buy_ratio": buy_ratio,
                "binance_spot_delta_score": delta_score,
                "mexc_futures_volume_ratio_30": float(mexc_volume_ratio_30 or 0.0),
                "ai_rule": "Use Binance spot as directional confirmation only; never penalize/boost because raw Binance spot volume is larger than MEXC futures volume."
            }
        except Exception as e:
            return {"error": str(e)[:120], "ai_rule": "ignore raw cross-exchange volume size"}


    def _market_data_fatal_error(self, market_data: dict) -> str:
        """Fail closed if MEXC/Binance inputs are missing.

        BTC AI mode trades on MEXC futures. Binance is used only for spot
        directional confirmation, but if that confirmation endpoint fails the
        bot must not open a trade because probability would be based on partial data.
        """
        checks = [
            ("MEXC funding", market_data.get("funding")),
            ("MEXC liquidation proxy", market_data.get("mexc_liquidations_proxy")),
            ("Binance spot pressure", market_data.get("binance_spot_pressure")),
        ]
        if not market_data.get("last_price"):
            return "MEXC ticker/last_price missing"
        if not market_data.get("mexc_orderbook"):
            return "MEXC orderbook missing"
        for name, obj in checks:
            if isinstance(obj, dict) and obj.get("error"):
                return f"{name}: {obj.get('error')}"
        return ""

    async def _recent_btc_ai_stop_losses(self, lookback_sec: int = 7 * 86400) -> int:
        """Count consecutive BTC AI stop-loss closes in recent history."""
        try:
            rows = await self.storage.trade_rows(since=time.time() - lookback_sec)
        except Exception:
            return 0
        btc_rows = []
        for r in rows:
            if str(r.get("strategy") or "") != "btc_ai_4h":
                continue
            sym = str(r.get("symbol") or "").upper()
            if sym not in {"BTC_USDT", "BTCUSDT", "BTC/USDT"}:
                continue
            btc_rows.append(r)
        btc_rows.sort(key=lambda x: float(x.get("ts_close") or 0), reverse=True)
        count = 0
        for r in btc_rows:
            reason = str(r.get("reason") or "").lower()
            result = str(r.get("result") or "").lower()
            is_stop = ("stop" in reason or reason in {"sl", "stop_loss"}) and result == "loss"
            if is_stop:
                count += 1
            else:
                break
        return count

    async def _apply_stop_loss_pause_if_needed(self, app) -> bool:
        """Pause BTC AI entries after 3 consecutive stop-losses.

        The pause is implemented by disabling the BTC AI entry switch. Pressing
        the BTC AI 4H button ON again clears this pause and resumes the next 4H cycle.
        """
        settings = await self.storage.all_settings()
        until = float(settings.get("btc_ai_pause_until", 0) or 0)
        if until > time.time():
            return True
        if int(await self._recent_btc_ai_stop_losses()) >= 3:
            until = time.time() + 24 * 3600
            await self.storage.set("btc_ai_pause_until", until, bump_revision=False)
            await self.storage.set("btc_ai_autopilot_enabled", False, bump_revision=False)
            log_event("btc_ai_pause_24h", reason="3_consecutive_stop_losses", pause_until=until, ok=False)
            await self._notify(app, "⏸ BTC AI 4H поставлен на паузу 24 часа: 3 сделки подряд закрылись по стопу.\nЧтобы снять паузу раньше — нажми кнопку BTC AI 4H ВКЛ еще раз.")
            return True
        return False

    def render_chart(self, symbol: str, candles: list, market_data: dict) -> str:
        df = self._prepare_chart_df(candles, tail=90)
        return self._draw_clean_btc_chart(df, market_data, levels=None, decision=None, filename_prefix="btc_ai_4h_clean")

    async def ask_ai(self, settings: dict, chart_path: str, market_data: dict) -> BTCAutopilotDecision:
        key = openai_key(settings)
        if not key:
            log_event("btc_ai_openai_error", ok=False, error="OpenAI API key missing")
            return BTCAutopilotDecision(error="OpenAI API key missing")
        model = active_model(settings)
        chart_bytes = Path(chart_path).read_bytes()
        img64 = base64.b64encode(chart_bytes).decode()
        chart_size_bytes = len(chart_bytes)
        prompt = """You are the dedicated BTC AI 4H autopilot risk engine. This prompt is ONLY for BTC_USDT 4H automated trading.

Execution venue: MEXC futures. Chart source: MEXC futures. Futures volume/funding/orderbook/liquidation proxy: MEXC. Binance data is SPOT confirmation only.

CRITICAL CROSS-EXCHANGE RULE:
Binance spot raw notional volume is normally much larger than MEXC futures volume. Do NOT compare raw Binance spot volume to raw MEXC futures volume. Do NOT lower or raise probability because Binance raw volume is bigger. Use MEXC data for executable futures structure and use Binance spot only as directional confirmation by buy_ratio/delta_score.

Return STRICT JSON only with numeric prices. Be conservative. If setup quality is weak, return WAIT and probability below 75.

Trading rules:
- BTC only.
- probability <75: no trade.
- 75-84.9: limit entry at entry zone midpoint.
- 85+: A+ setup, market order 100%.
- Entry zone must be realistic for the current 4H structure.
- AI may suggest stop, but bot will enforce stop distance between 1% and 2% from entry.
- Bot uses fixed TP1=2% close 50%, TP2=4% close remaining.
- If a trade is open longer than 24h, bot will close it only when price is breakeven or better; if negative, bot waits until breakeven.

Probability must include: MEXC 4H market structure, MEXC volume ratio, MEXC funding, MEXC orderbook, MEXC liquidation proxy, Binance spot directional confirmation, support/resistance, and risk/reward.

JSON schema: {"signal":"LONG|SHORT|WAIT","probability":0,"grade":"C|B|A|A+","entry_zone_low":0,"entry_zone_high":0,"stop_loss":0,"take_profit_1":0,"take_profit_2":0,"take_profit_3":0,"reason":"short"}"""
        prompt_with_data = prompt+"\nMarket JSON:\n"+json.dumps(market_data,ensure_ascii=False)
        prompt_size_chars = len(prompt_with_data)
        log_event(
            "btc_ai_prompt",
            model=model,
            prompt=prompt,
            market_data=market_data,
            chart_path=chart_path,
            prompt_size_chars=prompt_size_chars,
            chart_size_bytes=chart_size_bytes,
            chart_size_kb=round(chart_size_bytes/1024, 2),
            openai_image_detail="high",
        )
        payload={"model":model,"response_format":{"type":"json_object"},"messages":[{"role":"user","content":[{"type":"text","text":prompt_with_data},{"type":"image_url","image_url":{"url":"data:image/png;base64,"+img64,"detail":"high"}}]}],"max_completion_tokens":700}
        timeout=aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(OPENAI_CHAT_URL,headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},json=payload) as r:
                txt=await r.text()
                if r.status>=300:
                    # Compatibility fallback for older Chat models that still expect max_tokens.
                    if "max_completion_tokens" in txt and "unsupported" in txt.lower():
                        payload.pop("max_completion_tokens", None)
                        payload["max_tokens"] = 700
                        async with session.post(OPENAI_CHAT_URL,headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},json=payload) as r2:
                            txt=await r2.text()
                            if r2.status>=300:
                                log_event("btc_ai_openai_error", ok=False, status=r2.status, response=txt[:1000], fallback="max_tokens")
                                return BTCAutopilotDecision(error=f"OpenAI {r2.status}: {txt[:300]}")
                    else:
                        log_event("btc_ai_openai_error", ok=False, status=r.status, response=txt[:1000])
                        return BTCAutopilotDecision(error=f"OpenAI {r.status}: {txt[:300]}")
        try:
            response_json=json.loads(txt)
            usage=response_json.get("usage") or {}
            log_event(
                "btc_ai_openai_usage",
                model=model,
                ai_request_tokens=usage.get("prompt_tokens"),
                ai_response_tokens=usage.get("completion_tokens"),
                ai_total_tokens=usage.get("total_tokens"),
                prompt_size_chars=prompt_size_chars,
                chart_size_bytes=chart_size_bytes,
                chart_size_kb=round(chart_size_bytes/1024, 2),
            )
            raw=response_json["choices"][0]["message"]["content"]
            data=json.loads(raw)
            dec = BTCAutopilotDecision(signal=str(data.get("signal","WAIT")).upper(), probability=float(data.get("probability") or 0), grade=str(data.get("grade") or "C"), entry_zone_low=float(data.get("entry_zone_low") or 0), entry_zone_high=float(data.get("entry_zone_high") or 0), stop_loss=float(data.get("stop_loss") or 0), take_profit_1=float(data.get("take_profit_1") or 0), take_profit_2=float(data.get("take_profit_2") or 0), take_profit_3=float(data.get("take_profit_3") or 0), reason=str(data.get("reason") or "")[:600], raw=raw)
            log_event("btc_ai_decision", model=model, decision=dec.__dict__, raw=raw, usage=usage, prompt_size_chars=prompt_size_chars, chart_size_kb=round(chart_size_bytes/1024,2))
            return dec
        except Exception as e:
            log_event("btc_ai_openai_parse_error", ok=False, error=str(e), raw=txt[:1200])
            return BTCAutopilotDecision(error=f"AI parse error: {e}; raw={txt[:500]}")

    async def _mexc_funding(self, symbol):
        try:
            msym=self.exchange_client.mexc_contract_symbol(symbol)
            resp=await self.exchange_client._mexc_public("GET","/api/v1/contract/funding_rate/"+msym)
            d=resp.get("data") if isinstance(resp,dict) else {}
            return {"rate": float((d or {}).get("fundingRate") or 0), "nextSettleTime": (d or {}).get("nextSettleTime")}
        except Exception as e:
            log_event("btc_ai_mexc_error", ok=False, source="funding", error=str(e)[:300])
            return {"error": str(e)[:120]}

    async def _mexc_liquidation_proxy(self, symbol):
        # MEXC public liquidation history varies by region/API. Use open interest/ticker as a safe proxy when liquidation endpoint is unavailable.
        try:
            msym=self.exchange_client.mexc_contract_symbol(symbol)
            resp=await self.exchange_client._mexc_public("GET","/api/v1/contract/ticker",query={"symbol":msym})
            d=resp.get("data") if isinstance(resp,dict) else {}
            if isinstance(d,list): d=d[0] if d else {}
            return {"holdVol": float((d or {}).get("holdVol") or 0), "riseFallRate": float((d or {}).get("riseFallRate") or 0), "note":"proxy_not_real_liquidation_feed"}
        except Exception as e:
            log_event("btc_ai_mexc_error", ok=False, source="liquidation_proxy", error=str(e)[:300])
            return {"error": str(e)[:120]}

    async def _binance_spot_pressure(self, symbol="BTCUSDT"):
        try:
            end=int(time.time()*1000); start=end-15*60*1000
            url=f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start}&endTime={end}&limit=1000"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.get(url) as r:
                    data=await r.json()
            buy=sell=0.0
            for t in data if isinstance(data,list) else []:
                qty=float(t.get("q") or 0); price=float(t.get("p") or 0); notional=qty*price
                # m=True means buyer is maker => aggressive sell. m=False aggressive buy.
                if t.get("m"): sell += notional
                else: buy += notional
            total=buy+sell
            return {"buy_usdt":buy,"sell_usdt":sell,"delta_usdt":buy-sell,"buy_ratio":buy/total if total>0 else 0.5,"window_min":15}
        except Exception as e:
            log_event("btc_ai_binance_error", ok=False, source="spot_pressure", error=str(e)[:300])
            return {"error": str(e)[:120]}

    def _book_summary(self, book):
        """Robust MEXC orderbook summary.

        Some MEXC/ccxt responses return rows as [price, qty], others as
        [price, qty, count] or dicts. Never unpack rows directly, because
        that can raise: too many values to unpack (expected 2).
        """
        def _row_notional(row):
            try:
                if isinstance(row, dict):
                    p = row.get("price") or row.get("p") or row.get(0)
                    q = row.get("amount") or row.get("qty") or row.get("size") or row.get("q") or row.get(1)
                elif isinstance(row, (list, tuple)) and len(row) >= 2:
                    p, q = row[0], row[1]
                else:
                    return 0.0
                return float(p) * float(q)
            except Exception:
                return 0.0

        bids = (book or {}).get("bids") or []
        asks = (book or {}).get("asks") or []
        bid = sum(_row_notional(r) for r in bids[:20])
        ask = sum(_row_notional(r) for r in asks[:20])
        total = bid + ask
        return {"bid_usdt_top20": bid, "ask_usdt_top20": ask, "imbalance": (bid - ask) / total if total > 0 else 0}

    def format_decision(self,d,md,lv=None):
        if d.error: return f"❌ BTC AI error: {d.error}"
        lv = lv or {}
        entry_mid = float(lv.get("entry_mid") or 0)
        stop = float(lv.get("stop_loss") or 0)
        tp1 = float(lv.get("take_profit_1") or 0)
        tp2 = float(lv.get("take_profit_2") or 0)
        trade_status = "Сделка открыта" if d.signal in {"LONG","SHORT"} and float(d.probability or 0) >= 75 else "Сделка не открыта"
        return (f"{trade_status}: {d.signal}\n"
                f"Вход: {entry_mid:.2f}\n"
                f"Enter zone: {float(lv.get('entry_low') or d.entry_zone_low):.2f}-{float(lv.get('entry_high') or d.entry_zone_high):.2f}\n"
                f"Средняя цена лимитки: {entry_mid:.2f}\n"
                f"Стоп: {stop:.2f} ({float(lv.get('stop_pct') or 0):.2f}%)\n"
                f"Тейк1: {tp1:.2f} (+/-2%, закрыть 50%)\n"
                f"Тейк2: {tp2:.2f} (+/-4%, закрыть остаток)\n"
                f"Проходимость сделки по 4 часам: {d.probability:.1f}%\n"
                f"Свеча 4h, по закрытию: {md.get('closed_candle_msk')}\n"
                f"Дополнительно: виртуальное сопровождение включено\n"
                f"Spot buy ratio: {((md.get('binance_spot_pressure') or {}).get('buy_ratio') or 0):.2f}")

    async def _hard_disable_other_modes(self, settings):
        keys={"strategy_mode":"hybrid","max_open_positions":1,"live_trading":True,"openai_analysis_enabled":True,"boost_autopilot_active":False,"boost_parallel_scan_enabled":False,"ai_scalping_quality_filters_enabled":False,"liquidity_runner_enabled":False,"quick_bounce_enabled":False,"impulse_dump_enabled":False,"orderflow_impulse_enabled":False,"cascade_hunter_enabled":False,"strongest_coin_enabled":False,"auto_strategy_adaptation":False,"regime_adaptation":False,"spot_confirmation_enabled":False,"session_filter_enabled":False,"america_short_bias_enabled":False,"mirror_mode":"off","trade_margin_pct":0.10,"margin_allocation_enabled":True,"mexc_order_leverage":10,"limit_timeout_sec":14400,"scan_market_source":"mexc_binance"}
        for k,v in keys.items():
            try: await self.storage.set(k,v,bump_revision=False)
            except TypeError: await self.storage.set(k,v)

    async def _notify(self, app, text):
        try: await app.bot.send_message(chat_id=self._admin_id(), text=str(text)[:3900])
        except Exception: pass
    def _admin_id(self):
        ids=[x.strip() for x in str(os.getenv("ADMIN_IDS","")).split(",") if x.strip()]
        return ids[0] if ids else ""
    def _bool(self, s,k,d=False):
        v=s.get(k,d)
        return v if isinstance(v,bool) else str(v).lower() in {"1","true","yes","on"}
    def _fmt_msk(self, ts):
        return (datetime.fromtimestamp(float(ts),tz=timezone.utc)+timedelta(hours=3)).strftime("%Y-%m-%d %H:%M МСК")
