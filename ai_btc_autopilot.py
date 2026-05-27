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
        self._last_be_rate_limit_log_ts = 0.0
        self._last_be_warning_ts = 0.0
        self._last_24h_rate_limit_log_ts = 0.0
        self._last_24h_warning_ts = 0.0

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
        self._last_be_rate_limit_log_ts = 0.0
        self._last_be_warning_ts = 0.0
        self._last_24h_rate_limit_log_ts = 0.0
        self._last_24h_warning_ts = 0.0

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

    def _btc_symbol_match(self, value: Any) -> bool:
        v = str(value or "").upper().replace("/", "_").replace(":USDT", "")
        return v in {"BTC_USDT", "BTCUSDT"}

    def _exchange_position_contracts(self, ex_pos: dict) -> float:
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        for key in ("holdVol", "vol", "contracts"):
            try:
                val = info.get(key) if key in info else ex_pos.get(key)
                if val not in (None, ""):
                    return abs(float(val or 0))
            except Exception:
                pass
        return 0.0

    def _exchange_position_amount(self, ex_pos: dict) -> float:
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        for key in ("amount", "qty", "size"):
            try:
                val = ex_pos.get(key)
                if val not in (None, ""):
                    amount = abs(float(val or 0))
                    if amount > 0:
                        return amount
            except Exception:
                pass
        contracts = self._exchange_position_contracts(ex_pos)
        csize = 0.0
        for key in ("contractSize", "contract_size"):
            try:
                val = ex_pos.get(key) if key in ex_pos else info.get(key)
                if val not in (None, ""):
                    csize = abs(float(val or 0)); break
            except Exception:
                pass
        if csize <= 0 and self._btc_symbol_match(ex_pos.get("symbol") or info.get("symbol")):
            csize = 0.0001
        return contracts * csize if contracts > 0 and csize > 0 else contracts

    def _exchange_position_entry(self, ex_pos: dict) -> float:
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        for key in ("entryPrice", "holdAvgPrice", "openAvgPrice", "average", "avgPrice"):
            try:
                val = ex_pos.get(key) if key in ex_pos else info.get(key)
                if val not in (None, ""):
                    f = float(val or 0)
                    if f > 0:
                        return f
            except Exception:
                pass
        return 0.0

    def _exchange_position_side(self, ex_pos: dict) -> str:
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        raw = str(ex_pos.get("side") or info.get("positionType") or info.get("holdSide") or info.get("side") or "").strip().lower()
        if raw in {"long", "buy", "1"}:
            return "LONG"
        if raw in {"short", "sell", "2"}:
            return "SHORT"
        return raw.upper()

    def _exchange_position_opened_at(self, ex_pos: dict, local_pos: dict | None = None) -> float:
        if isinstance(local_pos, dict):
            for key in ("opened_at", "created_at", "entry_filled_at"):
                try:
                    val = float(local_pos.get(key) or 0)
                    if val > 0:
                        return val
                except Exception:
                    pass
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        for key in ("createTime", "createdTime", "created_at", "openTime"):
            try:
                val = info.get(key) if key in info else ex_pos.get(key)
                if val in (None, ""):
                    continue
                ts = float(val)
                if ts > 10_000_000_000:
                    ts /= 1000.0
                if ts > 0:
                    return ts
            except Exception:
                pass
        return 0.0

    async def _exchange_btc_positions(self) -> list[dict]:
        try:
            rows = await self.exchange_client.fetch_positions([self.symbol])
        except TypeError:
            rows = await self.exchange_client.fetch_positions()
        except Exception as e:
            log_event("btc_ai_exchange_positions_error", ok=False, error=str(e)[:600])
            return []
        out = []
        for row in rows or []:
            info = row.get("info") if isinstance(row.get("info"), dict) else {}
            if not (self._btc_symbol_match(row.get("symbol")) or self._btc_symbol_match(row.get("mexc_symbol")) or self._btc_symbol_match(info.get("symbol"))):
                continue
            if self._exchange_position_contracts(row) > 0 or self._exchange_position_amount(row) > 0:
                out.append(row)
        return out

    def _match_local_btc_position(self, local_positions: list[dict], ex_pos: dict) -> dict | None:
        ex_side = self._exchange_position_side(ex_pos)
        for pos in local_positions or []:
            if str(pos.get("strategy") or "") != "btc_ai_4h":
                continue
            if str(pos.get("status") or "").lower() not in {"open", "closing", "pending"}:
                continue
            if not self._btc_symbol_match(pos.get("symbol") or self.symbol):
                continue
            side = str(pos.get("side") or "").upper()
            if side and ex_side and side != ex_side:
                continue
            return pos
        return None

    def _exchange_stub_position(self, ex_pos: dict, local_pos: dict | None = None) -> dict:
        info = ex_pos.get("info") if isinstance(ex_pos.get("info"), dict) else {}
        entry = self._exchange_position_entry(ex_pos)
        side = self._exchange_position_side(ex_pos)
        amount = self._exchange_position_amount(ex_pos)
        contracts = self._exchange_position_contracts(ex_pos)
        base = dict(local_pos or {})
        base.update({
            "symbol": self.symbol,
            "side": side,
            "status": "open",
            "strategy": "btc_ai_4h",
            "entry_price": entry or float(base.get("entry_price") or 0),
            "qty": amount or float(base.get("qty") or 0),
            "exchange_contracts": contracts,
            "raw_exchange_position": ex_pos,
            "exchange_position_id": info.get("positionId") or base.get("exchange_position_id"),
        })
        opened_at = self._exchange_position_opened_at(ex_pos, base)
        if opened_at > 0:
            base.setdefault("opened_at", opened_at)
            base.setdefault("created_at", opened_at)
        return base

    async def active_btc_position(self) -> dict | None:
        """Exchange-first active BTC position lookup.

        Local storage is useful for plan metadata, but it is not the source of
        truth for live exposure.  If MEXC shows a BTC position, return it even
        when ACTIVE_POSITIONS is empty so the 4H cycle cannot open a second BTC
        trade over an existing exchange position.
        """
        try:
            ex_rows = await self._exchange_btc_positions()
        except Exception:
            ex_rows = []
        local_rows = []
        try:
            local_rows = await self.storage.positions()
        except Exception:
            local_rows = []
        if ex_rows:
            return self._exchange_stub_position(ex_rows[0], self._match_local_btc_position(local_rows, ex_rows[0]))
        for p in await self._btc_ai_positions():
            if str(p.get("status") or "").lower() == "open":
                return p
        return None

    async def _active_position_in_profit(self, pos: dict, market_data: dict | None = None) -> bool:
        """Return True only when the current BTC AI position is breakeven or better.

        Used for the 4H replacement rule: the bot may close an existing BTC AI
        position and open a fresh signal only when the old position is not losing.
        """
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return False
        side = str(pos.get("side") or "").upper()
        price = 0.0
        try:
            if market_data:
                price = float(market_data.get("last_price") or 0)
        except Exception:
            price = 0.0
        if price <= 0:
            ticker = await self.exchange_client.fetch_ticker(pos.get("symbol") or self.symbol)
            price = float(ticker.get("last") or 0)
        if price <= 0:
            return False
        if side == "LONG":
            return price >= entry
        if side == "SHORT":
            return price <= entry
        return False

    def _prob_tier(self, probability: float) -> str:
        """BTC AI probability hierarchy used for replacement rules."""
        try:
            p = float(probability or 0)
        except Exception:
            p = 0.0
        if p >= 85.0:
            return "HIGH"
        if p >= 75.0:
            return "MEDIUM"
        if p >= 65.0:
            return "LOW"
        return "NONE"

    def _position_probability(self, pos: dict) -> float:
        try:
            sd = pos.get("signal_details") or {}
            if isinstance(sd, dict) and sd.get("probability") is not None:
                return float(sd.get("probability") or 0)
        except Exception:
            pass
        try:
            return float(pos.get("confidence") or 0) * 100.0
        except Exception:
            return 0.0

    def _tp1_already_hit(self, pos: dict) -> bool:
        """Detect whether TP1 has already executed and SL was moved to breakeven."""
        if not isinstance(pos, dict):
            return False
        if pos.get("btc_ai_tp1_be_done") or pos.get("breakeven_moved") or pos.get("tp1_hit"):
            return True
        try:
            entry = float(pos.get("entry_price") or 0)
            stop = float(pos.get("stop_price") or 0)
            # If stop was already moved to BE, treat it as TP1 done even if the explicit flag is absent.
            if entry > 0 and stop > 0 and abs(stop - entry) / entry <= 0.00005:
                return True
        except Exception:
            pass
        return False

    def _replacement_decision(self, active: dict, new_probability: float, in_profit: bool, new_signal: str = "") -> tuple[bool, str]:
        """Final BTC AI replacement hierarchy.

        Rules requested:
        - If TP1 is executed: keep same-direction runner.
        - If TP1 is executed and a tradable opposite 4H signal appears (65%+), close runner and allow the new opposite trade.
        - Current 85%+ and new 65-84%: do not replace.
        - Current 85%+ and new 85%+: replace only if current is in profit.
        - Current 75-84% and new 85%+: replace immediately.
        - Current 75-84% and new 75-84%: replace only if current is in profit.
        - Current 65-74% and new 75%+: replace immediately.
        - Current 65-74% and new 65-74%: replace only if current is in profit.
        """
        current_side = str((active or {}).get("side") or "").upper()
        new_side = str(new_signal or "").upper()
        current_tier = self._prob_tier(self._position_probability(active))
        new_tier = self._prob_tier(new_probability)
        if self._tp1_already_hit(active):
            if new_side in {"LONG", "SHORT"} and current_side in {"LONG", "SHORT"} and new_side != current_side and new_tier != "NONE":
                return True, f"tp1_runner_closed_on_opposite_{new_tier.lower()}_signal"
            return False, "tp1_lock_same_direction_or_no_opposite_signal"
        if new_tier == "NONE":
            return False, f"new_signal_not_tradable:{new_tier}"
        if current_tier == "HIGH":
            if new_tier == "HIGH" and in_profit:
                return True, "high_to_high_in_profit"
            return False, f"current_high_keeps_priority:new_{new_tier}"
        if current_tier == "MEDIUM":
            if new_tier == "HIGH":
                return True, "medium_replaced_by_high"
            if new_tier == "MEDIUM" and in_profit:
                return True, "medium_to_medium_in_profit"
            return False, f"current_medium_not_replaceable:new_{new_tier}:in_profit_{in_profit}"
        if current_tier == "LOW":
            if new_tier in {"MEDIUM", "HIGH"}:
                return True, f"low_replaced_by_{new_tier.lower()}"
            if new_tier == "LOW" and in_profit:
                return True, "low_to_low_in_profit"
            return False, f"current_low_not_replaceable:new_{new_tier}:in_profit_{in_profit}"
        # Unknown old probability: be safe. Replace only by 75%+ when old position is in profit.
        if new_tier in {"MEDIUM", "HIGH"} and in_profit:
            return True, f"unknown_current_tier_replaced_by_{new_tier.lower()}_in_profit"
        return False, f"unknown_current_tier_keep:new_{new_tier}:in_profit_{in_profit}"

    async def _send_temp_ai_chart(self, app, chart_path: str, force_live_test: bool = False):
        """Send clean chart as a temporary progress message and delete it after AI finishes.

        This chart is the same clean chart sent to AI: no entry, no SL, no TP.
        If Telegram delete fails, it is silently ignored because trading logic must not depend on chat UI.
        """
        try:
            caption = "🧠 BTC AI анализирует чистый 4H график...\nЭтот график будет удалён после ответа ИИ."
            if force_live_test:
                caption = "🧪 BTC AI TEST: ИИ анализирует чистый 4H график...\nЭтот график будет удалён после ответа ИИ."
            with open(chart_path, "rb") as img:
                return await app.bot.send_photo(chat_id=self._admin_id(), photo=img, caption=caption[:1024])
        except Exception as e:
            log_event("btc_ai_temp_chart_send_error", ok=False, error=str(e)[:500])
            return None

    async def _delete_temp_message(self, app, msg):
        try:
            if msg is not None:
                await app.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id)
        except Exception as e:
            log_event("btc_ai_temp_chart_delete_warning", ok=False, error=str(e)[:300])

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
        if (not force_live_test) and await self._apply_stop_loss_pause_if_needed(app):
            return
        symbol = str(settings.get("btc_ai_symbol", self.symbol) or self.symbol)

        # Do not decide what to do with an existing active BTC position until the fresh
        # 4H AI signal is known. Final rule after AI decision:
        # - active position in loss: keep it, skip new entry;
        # - active position in profit and new signal >=75%: close old, open new;
        # - active position in profit but weak/no signal: keep it;
        # - pending limit without active position: cancel and replace with fresh signal.
        active_before_ai = await self.active_btc_position()
        if not active_before_ai:
            # If the previous 4H limit was not filled, replace it with a fresh signal.
            await self.cancel_pending_btc_entries(app, reason="btc_ai_4h_new_candle_replace")

        candles_raw = await self.exchange_client.fetch_ohlcv(symbol, timeframe="4h", limit=160)
        candles = self._closed_4h_candles(candles_raw)
        if len(candles) < 80:
            log_event("btc_ai_skip_not_enough_closed_4h_candles", ok=False, raw_count=len(candles_raw or []), closed_count=len(candles), min_required=80)
            await self._notify(app, f"⚠️ BTC AI: мало закрытых 4H свечей MEXC для анализа ({len(candles)}/80). Сделка не открывается.")
            return
        market_data = await self.collect_market_data(symbol, candles)
        fatal = self._market_data_fatal_error(market_data)
        if fatal:
            log_event("btc_ai_market_data_error", ok=False, error=fatal, market_data=market_data)
            await self._notify(app, f"❌ BTC AI: ошибка данных {fatal}. Сделка не открывается.")
            return

        # IMPORTANT: AI always gets a clean chart with no entry/SL/TP markup.
        ai_chart_path = await asyncio.to_thread(self.render_chart, symbol, candles, market_data)
        temp_ai_msg = await self._send_temp_ai_chart(app, ai_chart_path, force_live_test=force_live_test)
        try:
            decision = await self.ask_ai(settings, ai_chart_path, market_data)
        finally:
            # Delete the temporary clean chart from Telegram after AI has answered.
            await self._delete_temp_message(app, temp_ai_msg)
        log_event("btc_ai_chart_paths", ai_chart=ai_chart_path, mode="force_live_test" if force_live_test else "autopilot")

        if decision.error:
            if not force_live_test:
                await self._notify(app, f"❌ BTC AI error: {decision.error}")
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
                take_profit_1=last_price * 1.01 if last_price else 0,
                take_profit_2=last_price * 1.02 if last_price else 0,
                reason=f"LIVE TEST OVERRIDE after AI error: {decision.error}",
                error="",
            )

        prob = float(decision.probability or 0)
        reduced_mode_for_levels = (65.0 <= prob < 75.0) and (not force_live_test)
        forced_entry_for_levels = float(market_data.get("last_price") or 0) if (force_live_test or prob >= 85.0) else None
        plan_levels = self.prepare_levels(decision, market_data, forced_entry=forced_entry_for_levels, reduced_mode=reduced_mode_for_levels) if decision.signal in {"LONG", "SHORT"} else {}
        if decision.grade == "TEST":
            log_event("btc_ai_live_test_override", ok=True, reason="ai_error", decision=decision.__dict__, plan_levels=plan_levels)
        if force_live_test:
            last_price = float(market_data.get("last_price") or 0)
            original_signal = str(decision.signal or "WAIT").upper()
            original_probability = prob
            if decision.signal not in {"LONG", "SHORT"}:
                decision.signal = "LONG"
                decision.reason = (decision.reason + " | " if decision.reason else "") + "LIVE TEST OVERRIDE: AI signal was not tradable, forced LONG market to test MEXC mechanics."
            if float(decision.entry_zone_low or 0) <= 0 or float(decision.entry_zone_high or 0) <= 0:
                decision.entry_zone_low = last_price * 0.999 if last_price else 0
                decision.entry_zone_high = last_price * 1.001 if last_price else 0
            if float(decision.stop_loss or 0) <= 0 and last_price:
                decision.stop_loss = last_price * (0.99 if decision.signal == "LONG" else 1.01)
            if float(decision.take_profit_1 or 0) <= 0 and last_price:
                decision.take_profit_1 = last_price * (1.01 if decision.signal == "LONG" else 0.99)
            if float(decision.take_profit_2 or 0) <= 0 and last_price:
                decision.take_profit_2 = last_price * (1.02 if decision.signal == "LONG" else 0.98)
            decision.grade = str(decision.grade or "TEST") + " LIVE_TEST"
            plan_levels = self.prepare_levels(decision, market_data, forced_entry=last_price, reduced_mode=False)
            log_event("btc_ai_live_test_force_trade", ok=True, original_signal=original_signal, original_probability=original_probability, forced_signal=decision.signal, decision=decision.__dict__, plan_levels=plan_levels)

            # Telegram receives the annotated chart AFTER levels are calculated. AI did not see this chart.
            telegram_chart_path = await asyncio.to_thread(self.render_signal_chart, symbol, candles, market_data, decision, plan_levels)
            log_event("btc_ai_chart_paths", ai_chart=ai_chart_path, telegram_chart=telegram_chart_path, mode="live_test")
            caption = self.format_live_test_preview(decision, market_data, plan_levels, original_signal, original_probability)
            try:
                with open(telegram_chart_path, "rb") as img:
                    await app.bot.send_photo(chat_id=self._admin_id(), photo=img, caption=caption[:1024])
            except Exception:
                await self._notify(app, caption)
        elif decision.signal not in {"LONG", "SHORT"} or prob < float(settings.get("btc_ai_min_trade_probability", 65) or 65):
            await self._notify(app, f"🟡 BTC AI: сделки нет. AI signal={decision.signal}, проходимость={prob:.1f}%")
            return
        else:
            telegram_chart_path = await asyncio.to_thread(self.render_signal_chart, symbol, candles, market_data, decision, plan_levels)
            log_event("btc_ai_chart_paths", ai_chart=ai_chart_path, telegram_chart=telegram_chart_path, mode="autopilot_trade")
            caption = self.format_decision(decision, market_data, plan_levels)
            try:
                with open(telegram_chart_path, "rb") as img:
                    await app.bot.send_photo(chat_id=self._admin_id(), photo=img, caption=caption[:1024])
            except Exception:
                await self._notify(app, caption)

        # One BTC position max. Final replacement hierarchy:
        # - TP1 hit => keep runner, except close/reverse on a valid opposite 4H signal (65%+).
        # - HIGH(85+) is not replaced by LOW/MEDIUM.
        # - HIGH -> HIGH only if current position is in profit.
        # - MEDIUM -> HIGH immediately.
        # - MEDIUM -> MEDIUM only if current position is in profit.
        # - LOW -> MEDIUM/HIGH immediately.
        # - LOW -> LOW only if current position is in profit.
        active = await self.active_btc_position()
        if active:
            can_consider_replace = (not force_live_test) and decision.signal in {"LONG", "SHORT"} and prob >= 65.0
            in_profit = False
            try:
                in_profit = await self._active_position_in_profit(active, market_data)
            except Exception as e:
                log_event("btc_ai_active_profit_check_error", ok=False, error=str(e)[:500], active=active)
                in_profit = False
            replace_allowed, replace_reason = self._replacement_decision(active, prob, in_profit, decision.signal) if can_consider_replace else (False, "new_signal_not_replacement_candidate")
            log_event(
                "btc_ai_replacement_check",
                ok=True,
                replace_allowed=replace_allowed,
                replace_reason=replace_reason,
                current_probability=self._position_probability(active),
                current_tier=self._prob_tier(self._position_probability(active)),
                new_probability=prob,
                new_tier=self._prob_tier(prob),
                in_profit=in_profit,
                tp1_done=self._tp1_already_hit(active),
                active_position=active,
                new_signal=decision.__dict__,
            )
            if replace_allowed:
                live = self._bool(settings, "live_trading", False)
                try:
                    await self.exchange_client.cancel_all_orders(active.get("symbol") or self.symbol)
                except Exception as e:
                    log_event("btc_ai_replace_cancel_orders_warning", ok=False, error=str(e)[:500])
                try:
                    close_res = await self.execution_engine.close_position(active, reason=f"btc_ai_replacement_{replace_reason}", live=live, exit_price=float(market_data.get("last_price") or 0) or None)
                    log_event("btc_ai_replacement_closed_old_position", ok=True, close=close_res, old_position=active, new_signal=decision.__dict__, replace_reason=replace_reason)
                    await self._notify(app, f"♻️ BTC AI 4H: старая BTC позиция закрыта по правилу замены ({replace_reason}). Открываю новый 4H сигнал.")
                except Exception as e:
                    log_event("btc_ai_replacement_close_error", ok=False, error=str(e)[:1200], old_position=active, replace_reason=replace_reason)
                    await self._notify(app, f"❌ BTC AI: новая сделка не открыта — не смог закрыть старую позицию для замены: {str(e)[:250]}")
                    return
            else:
                if can_consider_replace:
                    await self._notify(app, f"⚠️ BTC AI 4H\n\nНовый сигнал {prob:.1f}% получен, но текущая BTC позиция не заменяется. Причина: {replace_reason}. Текущая сделка продолжает сопровождение.")
                else:
                    await self._notify(app, "⚠️ BTC AI 4H\n\nНовый анализ получен, но уже есть активная BTC позиция. Новый вход пропущен, текущая сделка продолжает сопровождение.")
                return
        # Race-safety: if an old pending is still present and there is no active position, cancel it before placing the new limit/market.
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
        prob = float(d.probability or 0)
        reduced_mode = (65.0 <= prob < 75.0) and (not force_market)
        balance_share = 0.05 if reduced_mode else float(settings.get("btc_ai_balance_share", 0.10) or 0.10)
        leverage = int(float(settings.get("btc_ai_leverage", 10) or 10))
        margin = total * balance_share
        notional = margin * leverage
        qty_total = notional / price
        lv = plan_levels or self.prepare_levels(d, market_data, forced_entry=price if d.probability >= 85 else None, reduced_mode=reduced_mode)
        entry_low, entry_high = float(lv.get("entry_low") or 0), float(lv.get("entry_high") or 0)
        entry_mid = float(lv.get("entry_mid") or 0)
        stop = float(lv.get("stop_loss") or 0)
        tp1 = float(lv.get("take_profit_1") or 0)
        tp2 = float(lv.get("take_profit_2") or 0)
        if tp1 <= 0 or stop <= 0 or entry_mid <= 0 or ((not reduced_mode) and tp2 <= 0):
            await self._notify(app, "❌ BTC AI: уровни ИИ не прошли risk-check: нужен entry, SL, TP1; для 75%+ нужен ещё TP2")
            return
        maxpos = 1
        tp1_fraction = 1.0 if reduced_mode else 0.50
        common = {
            "btc_ai": True,
            "probability": d.probability,
            "entry_zone": [entry_low, entry_high],
            "cancel_after_sec": 14400,
            "reason": d.reason,
            "risk_mode": ("reduced_65_74" if reduced_mode else "normal_75_plus"),
            "balance_share": balance_share,
            "tp1_fraction": tp1_fraction,
            "move_sl_to_be_after_tp1": (not reduced_mode),
            "tp_source": "ai",
            "bot_does_not_modify_tp": True,
            "stop_adjusted_by_bot": bool(lv.get("stop_adjusted_by_bot")),
            "ai_stop_pct": float(lv.get("ai_stop_pct") or 0),
            "final_stop_pct": float(lv.get("stop_pct") or 0),
        }
        if force_market or d.probability >= 85:
            # Normal live 85%+ market keeps split TP1/TP2.
            # /test_btc may force market below threshold, but it is a mechanical test only.
            market_tp_fraction = 0.50 if not reduced_mode else 1.0
            market_final_tp = tp2 if not reduced_mode else tp1
            plan_m = TradePlan(symbol=symbol, side=d.signal, order_type="market", qty=qty_total, entry_price=price, stop_price=stop, take_price=market_final_tp, partial_take_price=tp1, partial_take_fraction=market_tp_fraction, final_take_price=market_final_tp, risk_pct=0.0, confidence=d.probability/100, strategy="btc_ai_4h", max_open_positions=maxpos, planned_notional_usdt=notional, expected_margin_usdt=margin, leverage=leverage, signal_details=common)
            log_event("btc_ai_order_request", symbol=symbol, side=d.signal, order_type="market", force_live_test=force_market, qty=qty_total, entry=price, stop=stop, tp1=tp1, tp2=tp2, probability=d.probability, margin=margin, leverage=leverage, venue="MEXC")
            res_m = await self.execution_engine.place_entry(plan_m, live=live)
            log_event("btc_ai_order_response", symbol=symbol, side=d.signal, order_type="market", response=res_m, ok=bool(isinstance(res_m, dict) and res_m.get("ok", True)))
            
            status_ok = bool(isinstance(res_m, dict) and res_m.get("ok", True))
            if not status_ok:
                reason = ""
                if isinstance(res_m, dict):
                    reason = str(res_m.get("reason") or res_m.get("error") or res_m)[:350]
                else:
                    reason = str(res_m)[:350]
                await self._notify(app, f"❌ BTC AI {'LIVE TEST' if force_market else 'A+'} MARKET НЕ ОТКРЫТ\n"
                                        f"Причина: {reason}\n"
                                        f"Позиция/ордера не изменены. Подробности: /log")
                return
            protection = "EXCHANGE PROTECTED"
            if reduced_mode:
                await self._notify(app, f"✅ BTC AI {'LIVE TEST' if force_market else 'MARKET'} REDUCED 65–74%\n"
                                        f"Вход: ~{price:.2f}\n"
                                        f"SL: {stop:.2f} ({float(lv.get('stop_pct') or 0):.2f}%)\n"
                                        f"TP: {tp1:.2f} (ИИ, закрыть 100%)\n"
                                        f"Проходимость: {d.probability:.1f}%\n"
                                        f"Размер: 5% от total balance · x{leverage}\n"
                                        f"Защита: {protection}\n"
                                        f"Виртуальное сопровождение: ВКЛ")
            else:
                await self._notify(app, f"✅ BTC AI {'LIVE TEST' if force_market else 'A+'} MARKET 100%\n"
                                        f"Вход: ~{price:.2f}\n"
                                        f"SL: {stop:.2f}\n"
                                        f"TP1: {tp1:.2f} (ИИ, 50%)\n"
                                        f"TP2: {tp2:.2f} (ИИ, остаток)\n"
                                        f"Проходимость: {d.probability:.1f}%\n"
                                        f"Защита: {protection}\n"
                                        f"Виртуальное сопровождение: ВКЛ")
            return
        final_take = tp1 if reduced_mode else tp2
        plan = TradePlan(symbol=symbol, side=d.signal, order_type="limit", qty=qty_total, entry_price=entry_mid, stop_price=stop, take_price=final_take, partial_take_price=tp1, partial_take_fraction=tp1_fraction, final_take_price=final_take, risk_pct=0.0, confidence=d.probability/100, strategy="btc_ai_4h", max_open_positions=maxpos, planned_notional_usdt=notional, expected_margin_usdt=margin, leverage=leverage, signal_details=common)
        log_event("btc_ai_order_request", symbol=symbol, side=d.signal, order_type="limit", qty=qty_total, entry=entry_mid, entry_zone=[entry_low, entry_high], stop=stop, tp1=tp1, tp2=tp2, probability=d.probability, margin=margin, leverage=leverage, venue="MEXC")
        res = await self.execution_engine.place_entry(plan, live=live)
        log_event("btc_ai_order_response", symbol=symbol, side=d.signal, order_type="limit", response=res, ok=bool(isinstance(res, dict) and res.get("ok", True)))
        
        limit_ok = bool(isinstance(res, dict) and res.get("ok", True))
        if not limit_ok:
            reason = ""
            if isinstance(res, dict):
                reason = str(res.get("reason") or res.get("error") or res)[:350]
            else:
                reason = str(res)[:350]
            await self._notify(app, f"❌ BTC AI LIMIT НЕ ВЫСТАВЛЕН\n"
                                    f"Причина: {reason}\n"
                                    f"Позиция/ордера не изменены. Подробности: /log")
            return
        if reduced_mode:
            await self._notify(app, f"✅ BTC AI LIMIT выставлен · REDUCED 65–74%\n"
                                    f"Вход: {entry_mid:.2f}\n"
                                    f"SL: {stop:.2f} ({float(lv.get('stop_pct') or 0):.2f}%)\n"
                                    f"TP: {tp1:.2f} (ИИ, закрыть 100%)\n"
                                    f"Проходимость: {d.probability:.1f}%\n"
                                    f"Размер: 5% от total balance · x{leverage}\n"
                                    f"Лимитка живет: 4 часа\n"
                                    f"Виртуальное сопровождение: ВКЛ")
        else:
            await self._notify(app, f"✅ BTC AI LIMIT выставлен\n"
                                    f"Вход: {entry_mid:.2f}\n"
                                    f"SL: {stop:.2f}\n"
                                    f"TP1: {tp1:.2f} (ИИ, 50%)\n"
                                    f"TP2: {tp2:.2f} (ИИ, остаток)\n"
                                    f"Проходимость: {d.probability:.1f}%\n"
                                    f"Размер: 10% от total balance · x{leverage}\n"
                                    f"Лимитка живет: 4 часа\n"
                                    f"Виртуальное сопровождение: ВКЛ")

    def prepare_levels(self, d: BTCAutopilotDecision, market_data: dict, forced_entry: float | None = None, reduced_mode: bool = False) -> dict:
        """Build final executable levels from the AI plan.

        Design rule for BTC AI:
        - AI owns ENTRY zone and TP1/TP2. The bot must not rewrite take-profits.
        - Bot validates direction and risk.
        - Bot may only widen a too-close stop to 1% from executable entry.
        - If the AI stop needs more than 2% room, reject the setup instead of distorting it.
        """
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
            ai_stop = float(d.stop_loss or 0)
            ai_tp1 = float(d.take_profit_1 or 0)
            ai_tp2 = float(d.take_profit_2 or 0)
            if ai_stop <= 0 or ai_tp1 <= 0:
                log_event("btc_ai_levels_rejected", reason="missing_ai_sl_or_tp1", decision=d.__dict__, reduced_mode=reduced_mode)
                return {}
            if (not reduced_mode) and ai_tp2 <= 0:
                log_event("btc_ai_levels_rejected", reason="missing_ai_tp2_for_75_plus", decision=d.__dict__, reduced_mode=reduced_mode)
                return {}

            # Validate geometry. Reduced 65-74% uses one AI TP only. 75%+ requires two AI TPs.
            if side == "LONG":
                if reduced_mode:
                    ok = ai_stop < entry_mid < ai_tp1
                else:
                    ok = ai_stop < entry_mid < ai_tp1 <= ai_tp2
            else:
                if reduced_mode:
                    ok = ai_tp1 < entry_mid < ai_stop
                else:
                    ok = ai_tp2 <= ai_tp1 < entry_mid < ai_stop
            if not ok:
                log_event(
                    "btc_ai_levels_rejected",
                    reason="invalid_ai_level_geometry",
                    side=side, entry_mid=entry_mid, stop_loss=ai_stop, tp1=ai_tp1, tp2=ai_tp2,
                    reduced_mode=reduced_mode, decision=d.__dict__,
                )
                return {}

            stop_pct_ai = abs(entry_mid - ai_stop) / entry_mid * 100.0
            if stop_pct_ai < 1.0:
                # Only allowed correction: widen SL to a minimum 1% from executable entry.
                stop = entry_mid * (0.99 if side == "LONG" else 1.01)
                stop_pct = 1.0
                stop_adjusted = True
            elif stop_pct_ai <= 2.0:
                stop = ai_stop
                stop_pct = stop_pct_ai
                stop_adjusted = False
            else:
                # Do not compress a wide AI stop into a different trade idea. Skip the signal.
                log_event(
                    "btc_ai_levels_rejected",
                    reason="ai_stop_wider_than_2pct",
                    side=side, entry_mid=entry_mid, stop_loss=ai_stop, stop_pct=stop_pct_ai,
                    reduced_mode=reduced_mode, decision=d.__dict__,
                )
                return {}

            tp1 = ai_tp1
            tp2 = 0.0 if reduced_mode else ai_tp2
            tp1_pct = abs(tp1 - entry_mid) / entry_mid * 100.0 if entry_mid > 0 else 0.0
            tp2_pct = abs(tp2 - entry_mid) / entry_mid * 100.0 if (entry_mid > 0 and tp2 > 0) else 0.0
            return {
                "entry_low": entry_low,
                "entry_high": entry_high,
                "entry_mid": entry_mid,
                "stop_loss": stop,
                "take_profit_1": tp1,
                "take_profit_2": tp2,
                "stop_pct": stop_pct,
                "ai_stop_pct": stop_pct_ai,
                "stop_adjusted_by_bot": stop_adjusted,
                "tp1_pct": tp1_pct,
                "tp2_pct": tp2_pct,
                "reduced_mode": reduced_mode,
                "tp_source": "ai",
            }
        except Exception as e:
            log_event("btc_ai_levels_error", ok=False, error=str(e)[:500], decision=getattr(d, "__dict__", {}))
            return {}

    def _closed_4h_candles(self, candles: list) -> list:
        """Return only confirmed closed 4H candles and log the decision.

        MEXC can include the currently forming 4H candle as the last OHLCV row.
        BTC AI must analyse closed candles only, so a candle is accepted only when:
            candle_open_time + 4h <= current_server_time
        No positive time tolerance is used: a candle is never treated as closed early.
        If the filtered result is too short, the caller will skip trading instead of
        silently falling back to unfiltered data.
        """
        raw = list(candles or [])
        out = []
        now_ms = int(time.time() * 1000)
        tf_ms = 4 * 60 * 60 * 1000
        last_raw_ts = None
        last_used_ts = None
        parse_errors = 0

        for row in raw:
            try:
                ts = int(float(row[0] or 0))
                ts = ts * 1000 if ts < 10_000_000_000 else ts
                last_raw_ts = ts
                if ts + tf_ms <= now_ms:
                    out.append(row)
                    last_used_ts = ts
            except Exception:
                parse_errors += 1
                continue

        dropped = max(0, len(raw) - len(out) - parse_errors)
        try:
            log_event(
                "btc_ai_closed_4h_filter",
                ok=bool(out),
                raw_count=len(raw),
                closed_count=len(out),
                dropped_open_or_future=dropped,
                parse_errors=parse_errors,
                now_ms=now_ms,
                last_raw_ts=last_raw_ts,
                last_raw_iso=(datetime.fromtimestamp(last_raw_ts / 1000, tz=timezone.utc).isoformat() if last_raw_ts else None),
                last_used_ts=last_used_ts,
                last_used_iso=(datetime.fromtimestamp(last_used_ts / 1000, tz=timezone.utc).isoformat() if last_used_ts else None),
            )
        except Exception:
            pass
        return out

    def _prepare_chart_df(self, candles: list, tail: int = 90) -> pd.DataFrame:
        df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume"])
        df["dt"] = pd.to_datetime(df.ts, unit="ms")
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = df[c].astype(float)
        # Compute indicators on the full history first, then crop. Otherwise MA99
        # is always NaN on a 90-candle chart and the AI loses long-trend context.
        df["MA7"] = df.close.rolling(7).mean()
        df["MA25"] = df.close.rolling(25).mean()
        df["MA99"] = df.close.rolling(99).mean()
        ema12 = df.close.ewm(span=12, adjust=False).mean()
        ema26 = df.close.ewm(span=26, adjust=False).mean()
        df["MACD"] = ema12 - ema26
        df["Signal"] = df.MACD.ewm(span=9, adjust=False).mean()
        df["Hist"] = df.MACD - df.Signal
        return df.tail(tail).reset_index(drop=True)

    def _draw_clean_btc_chart(self, df: pd.DataFrame, market_data: dict, levels: dict | None = None, decision: BTCAutopilotDecision | None = None, filename_prefix: str = "btc_ai_clean") -> str:
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
        from matplotlib.ticker import FuncFormatter

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
        high24_for_scale = float(market_data.get("high_24h") or 0)
        low24_for_scale = float(market_data.get("low_24h") or 0)
        if high24_for_scale > 0:
            all_price_levels.append(high24_for_scale)
        if low24_for_scale > 0:
            all_price_levels.append(low24_for_scale)
        if levels:
            all_price_levels += [float(levels.get(k) or 0) for k in ["entry_mid", "stop_loss", "take_profit_1", "take_profit_2"]]
        all_price_levels = [v for v in all_price_levels if v > 0]
        ymin, ymax = min(all_price_levels), max(all_price_levels)
        pad = max((ymax - ymin) * 0.12, last * 0.003)
        ax.set_ylim(ymin - pad, ymax + pad)
        ax.set_xlim(-1, len(df) + 12)

        # Current price and 24H range labels. These are visible on the AI clean chart too.
        ax.axhline(last, color=txt, linestyle=":", linewidth=1.1, alpha=0.75)
        ax.text(len(df) + 0.4, last, f"LAST {last:.1f}", color=txt, va="center", fontsize=9,
                bbox=dict(boxstyle="round,pad=0.25", facecolor="#111827", edgecolor=txt, alpha=0.75))

        # Clean AI chart: mark the real 24H extremes on the candles that made them.
        # On 4H BTC chart, 24 hours = last 6 candles. Do not place labels at a fixed X offset,
        # because that makes HIGH/LOW look random and can confuse visual AI analysis.
        lookback_24h = min(len(df), 6)
        high24 = 0.0
        low24 = 0.0
        high24_idx = None
        low24_idx = None
        if lookback_24h > 0:
            df24 = df.tail(lookback_24h)
            high24_idx = int(df24["high"].idxmax())
            low24_idx = int(df24["low"].idxmin())
            high24 = float(df.loc[high24_idx, "high"])
            low24 = float(df.loc[low24_idx, "low"])
        if high24 > 0 and high24_idx is not None:
            ax.axhline(high24, color="#94a3b8", linestyle=":", linewidth=0.8, alpha=0.42)
            ax.scatter([high24_idx], [high24], marker="^", s=42, color="#cbd5e1", edgecolor="#0b111c", linewidth=0.7, zorder=6)
            ax.annotate(f"24H HIGH {high24:.1f}", xy=(high24_idx, high24), xytext=(0, 12), textcoords="offset points",
                        color="#cbd5e1", va="bottom", ha="center", fontsize=8,
                        bbox=dict(boxstyle="round,pad=0.18", facecolor="#0b111c", edgecolor="#334155", alpha=0.70),
                        arrowprops=dict(arrowstyle="-", color="#64748b", alpha=0.65, linewidth=0.8))
        if low24 > 0 and low24_idx is not None:
            ax.axhline(low24, color="#94a3b8", linestyle=":", linewidth=0.8, alpha=0.42)
            ax.scatter([low24_idx], [low24], marker="v", s=42, color="#cbd5e1", edgecolor="#0b111c", linewidth=0.7, zorder=6)
            ax.annotate(f"24H LOW {low24:.1f}", xy=(low24_idx, low24), xytext=(0, -14), textcoords="offset points",
                        color="#cbd5e1", va="top", ha="center", fontsize=8,
                        bbox=dict(boxstyle="round,pad=0.18", facecolor="#0b111c", edgecolor="#334155", alpha=0.70),
                        arrowprops=dict(arrowstyle="-", color="#64748b", alpha=0.65, linewidth=0.8))

        if levels:
            entry = float(levels.get("entry_mid") or 0)
            sl = float(levels.get("stop_loss") or 0)
            tp1 = float(levels.get("take_profit_1") or 0)
            tp2 = float(levels.get("take_profit_2") or 0)
            reduced_chart = bool(levels.get("reduced_mode")) or (abs(tp2 - tp1) <= max(1e-9, abs(tp1) * 1e-8) if tp1 > 0 and tp2 > 0 else False)
            e_low = float(levels.get("entry_low") or entry)
            e_high = float(levels.get("entry_high") or entry)
            span_left = 0.58
            if entry > 0 and sl > 0:
                ax.axhspan(min(entry, sl), max(entry, sl), xmin=span_left, xmax=1.0, color=red, alpha=0.12)
            if entry > 0 and tp1 > 0:
                shade_top = tp1 if reduced_chart else (tp2 if tp2 > 0 else tp1)
                ax.axhspan(min(entry, shade_top), max(entry, shade_top), xmin=span_left, xmax=1.0, color=green, alpha=0.10)
            if e_low > 0 and e_high > 0:
                ax.axhspan(min(e_low, e_high), max(e_low, e_high), xmin=span_left, xmax=1.0, color=orange, alpha=0.18)
            if reduced_chart:
                level_rows = [(entry, "ENTRY", orange), (sl, "SL", red), (tp1, "TP AI 100%", green)]
            else:
                level_rows = [(entry, "ENTRY", orange), (sl, "SL", red), (tp1, "TP1 AI", green), (tp2, "TP2 AI", green)]
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
        def _compact_volume_formatter(value, _pos):
            value = float(value or 0)
            sign = "-" if value < 0 else ""
            value = abs(value)
            if value >= 1_000_000_000:
                return f"{sign}{value / 1_000_000_000:.1f}B"
            if value >= 1_000_000:
                return f"{sign}{value / 1_000_000:.0f}M"
            if value >= 1_000:
                return f"{sign}{value / 1_000:.0f}K"
            return f"{sign}{value:.0f}"
        av.yaxis.set_major_formatter(FuncFormatter(_compact_volume_formatter))
        av.yaxis.get_offset_text().set_visible(False)
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
        """Exchange-first TP1 monitor.

        Source of truth is MEXC /position/open_positions. Local storage is used
        only as metadata for AI TP1/TP2/entry values. The remaining size comes
        from the live exchange position, so a stale local qty cannot break BE
        protection after TP1.
        """
        try:
            exchange_positions = await self._exchange_btc_positions()
            if not exchange_positions:
                return
            try:
                local_positions = await self.storage.positions()
            except Exception:
                local_positions = []

            for ex_pos in exchange_positions:
                local = self._match_local_btc_position(local_positions, ex_pos)
                if not local:
                    log_event("btc_ai_be_exchange_position_no_local_metadata", ok=False, symbol=self.symbol, exchange_position=ex_pos)
                    continue
                if local.get("btc_ai_tp1_be_done") or local.get("breakeven_moved"):
                    continue

                sig = local.get("signal_details") if isinstance(local.get("signal_details"), dict) else {}
                if str(sig.get("risk_mode") or "") == "reduced_65_74" or float(local.get("partial_take_fraction") or 0) >= 0.999:
                    # One-TP reduced setup closes 100%; there is no runner to move to BE.
                    continue

                tp1 = float(local.get("partial_take_price") or 0)
                tp2 = float(local.get("final_take_price") or local.get("take_price") or 0)
                entry = self._exchange_position_entry(ex_pos) or float(local.get("entry_price") or 0)
                if tp1 <= 0 or tp2 <= 0 or entry <= 0:
                    log_event("btc_ai_be_missing_ai_levels", ok=False, tp1=tp1, tp2=tp2, entry=entry, local_id=local.get("id"))
                    continue

                side = self._exchange_position_side(ex_pos) or str(local.get("side") or "").upper()
                ticker = await self.exchange_client.fetch_ticker(local.get("symbol") or self.symbol)
                price = float(ticker.get("last") or 0)
                touched = (side == "LONG" and price >= tp1) or (side == "SHORT" and price <= tp1)
                if not touched:
                    continue

                # If the original TP1 trigger is still visible, do not move the stop yet.
                tp1_id = str(local.get("tp1_order_id") or "")
                if tp1_id and hasattr(self.exchange_client, "mexc_find_active_plan_order"):
                    try:
                        row = await self.exchange_client.mexc_find_active_plan_order(local.get("symbol") or self.symbol, order_id=tp1_id)
                        if row:
                            continue
                    except Exception:
                        # Endpoint/rate issues should not block BE forever once price touched TP1.
                        pass

                remaining_qty = self._exchange_position_amount(ex_pos)
                remaining_contracts = self._exchange_position_contracts(ex_pos)
                if remaining_qty <= 0 and remaining_contracts <= 0:
                    continue

                close_side = "sell" if side == "LONG" else "buy"
                try:
                    await self.exchange_client.cancel_all_orders(local.get("symbol") or self.symbol)
                except Exception as e:
                    log_event("btc_ai_be_cancel_old_protection_warning", ok=False, error=str(e)[:300])

                # Use live exchange amount for the runner.  Never use stale local qty.
                if remaining_qty <= 0:
                    remaining_qty = float(local.get("qty") or 0)
                if remaining_qty <= 0:
                    log_event("btc_ai_be_no_remaining_qty", ok=False, contracts=remaining_contracts, local_qty=local.get("qty"))
                    continue

                tp_order = sl_order = None
                try:
                    tp_order = await self.execution_engine._create_take_profit_market_order(local.get("symbol") or self.symbol, close_side, remaining_qty, tp2)
                    sl_order = await self.execution_engine._create_stop_market_order(local.get("symbol") or self.symbol, close_side, remaining_qty, entry)
                except Exception as e:
                    log_event("btc_ai_be_reprotect_failed", ok=False, error=str(e)[:800], remaining_qty=remaining_qty, entry=entry, tp2=tp2)
                    await self._notify(app, f"⚠️ BTC AI TP1: не смог переставить SL в Б/У по бирже. Проверь MEXC. Ошибка: {str(e)[:250]}")
                    continue

                local["qty"] = remaining_qty
                local["exchange_contracts"] = remaining_contracts
                local["raw_exchange_position"] = ex_pos
                local["stop_price"] = entry
                local["breakeven_moved"] = True
                local["btc_ai_tp1_be_done"] = True
                local["be_tp2_order_id"] = (tp_order or {}).get("id")
                local["be_sl_order_id"] = (sl_order or {}).get("id")
                local["updated_at"] = time.time()
                await self.storage.upsert_position(local)
                await self._notify(app, f"🟢 BTC AI TP1 взят по бирже. Остаток {remaining_contracts:g} контрактов защищён: SL в Б/У {entry:.2f}, TP2 ИИ {tp2:.2f}")
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            if "510" in msg or "too frequent" in low or "rate" in low:
                now = time.time()
                if now - self._last_be_rate_limit_log_ts > 300:
                    self._last_be_rate_limit_log_ts = now
                    log_event("btc_ai_be_monitor_rate_limited", ok=False, error=msg[:600], action="silent_retry")
                return
            now = time.time()
            if now - self._last_be_warning_ts > 300:
                self._last_be_warning_ts = now
                log_event("btc_ai_be_monitor_warning", ok=False, error=msg[:600])
                await self._notify(app, f"⚠️ BTC AI BE monitor warning: {msg[:300]}")

    async def monitor_24h_time_exit(self, app):
        """Exchange-first 24H time exit.

        After 24h the bot checks the real BTC position on MEXC, not local cache.
        If the live position is breakeven or in profit, it closes the exchange
        holdVol by market. If it is losing, it waits until breakeven.
        """
        try:
            exchange_positions = await self._exchange_btc_positions()
            if not exchange_positions:
                return
            try:
                local_positions = await self.storage.positions()
            except Exception:
                local_positions = []
            settings = await self.storage.all_settings()
            live = self._bool(settings, "live_trading", False)

            for ex_pos in exchange_positions:
                local = self._match_local_btc_position(local_positions, ex_pos)
                pos = self._exchange_stub_position(ex_pos, local)
                if pos.get("btc_ai_24h_exit_done"):
                    continue
                opened_at = self._exchange_position_opened_at(ex_pos, local)
                if opened_at <= 0 or time.time() - opened_at < 86400:
                    continue

                entry = self._exchange_position_entry(ex_pos) or float(pos.get("entry_price") or 0)
                if entry <= 0:
                    continue
                side = self._exchange_position_side(ex_pos) or str(pos.get("side") or "").upper()
                ticker = await self.exchange_client.fetch_ticker(pos.get("symbol") or self.symbol)
                price = float(ticker.get("last") or 0)
                if price <= 0:
                    continue
                pnl_ok = (side == "LONG" and price >= entry) or (side == "SHORT" and price <= entry)
                if not pnl_ok:
                    if local and not local.get("btc_ai_24h_wait_be_notified"):
                        local["btc_ai_24h_wait_be_notified"] = True
                        local["btc_ai_24h_wait_be_started_at"] = time.time()
                        await self.storage.upsert_position(local)
                    await self._notify(app, f"⏳ BTC AI 24H: реальная позиция MEXC открыта больше суток, но сейчас в минусе. Не закрываю. Закрою, когда цена вернется в Б/У: {entry:.2f}")
                    continue

                if not live:
                    log_event("btc_ai_24h_exchange_close_blocked_live_false", ok=False, symbol=self.symbol, entry=entry, price=price, side=side)
                    await self._notify(app, "⚠️ BTC AI 24H: на MEXC есть позиция в Б/У/плюсе, но live_trading=false. Биржевое автозакрытие не выполнено.")
                    continue

                try:
                    await self.exchange_client.cancel_all_orders(pos.get("symbol") or self.symbol)
                except Exception as e:
                    log_event("btc_ai_24h_cancel_protection_warning", ok=False, error=str(e)[:300])

                # Close the exact live exchange row/holdVol. Do not depend on local qty.
                res = await self.execution_engine.close_exchange_position(ex_pos, reason="btc_ai_24h_exchange_breakeven_time_exit")
                if not isinstance(res, dict) or not res.get("ok", False):
                    log_event("btc_ai_24h_exchange_close_failed", ok=False, response=res, entry=entry, price=price, exchange_position=ex_pos)
                    await self._notify(app, f"⚠️ BTC AI 24H: не смог закрыть реальную позицию MEXC. Ответ: {str(res)[:300]}")
                    continue

                if local:
                    local["btc_ai_24h_exit_done"] = True
                    local["status"] = "closed"
                    local["closed_at"] = time.time()
                    local["exit_price"] = price
                    local["close_reason"] = "btc_ai_24h_exchange_breakeven_time_exit"
                    local["updated_at"] = time.time()
                    try:
                        await self.storage.upsert_position(local)
                    except Exception:
                        pass
                await self._notify(app, f"⏰ BTC AI 24H: реальная позиция MEXC открыта больше суток и вышла в Б/У/плюс. Закрыл по рынку. Entry {entry:.2f}, current {price:.2f}. Детали в /log")
        except Exception as e:
            msg = str(e)
            low = msg.lower()
            if "510" in msg or "too frequent" in low or "rate" in low:
                now = time.time()
                if now - self._last_24h_rate_limit_log_ts > 300:
                    self._last_24h_rate_limit_log_ts = now
                    log_event("btc_ai_24h_monitor_rate_limited", ok=False, error=msg[:600], action="silent_retry")
                return
            now = time.time()
            if now - self._last_24h_warning_ts > 300:
                self._last_24h_warning_ts = now
                log_event("btc_ai_24h_monitor_warning", ok=False, error=msg[:800])
                await self._notify(app, f"⚠️ BTC AI 24H monitor warning: {msg[:300]}")

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
        ticker_info = ticker.get("info") if isinstance(ticker, dict) else {}
        if not isinstance(ticker_info, dict):
            ticker_info = {}
        depth = await self.exchange_client.fetch_order_book(symbol, limit=50)
        funding = await self._mexc_funding(symbol)
        spot = await self._binance_spot_pressure("BTCUSDT")
        liq = await self._mexc_liquidation_proxy(symbol)
        df = pd.DataFrame(candles, columns=["ts","open","high","low","close","volume"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
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
            "high_24h": float(df.high.tail(6).max()),
            "low_24h": float(df.low.tail(6).min()),
            "mexc_ticker_high_24h": float(ticker_info.get("high24Price") or ticker_info.get("high24") or ticker_info.get("high") or 0),
            "mexc_ticker_low_24h": float(ticker_info.get("lower24Price") or ticker_info.get("low24Price") or ticker_info.get("low24") or ticker_info.get("low") or 0),
            "mexc_volume_last": float(df.volume.iloc[-1]),
            "mexc_volume_ratio_30": vol_ratio,
            "funding": funding,
            "mexc_orderbook": self._book_summary(depth),
            "binance_spot_pressure": spot,
            "cross_exchange_pressure_normalized": spot_norm,
            "mexc_liquidations_proxy": liq,
            "closed_candle_msk": self._fmt_msk(float(df.ts.iloc[-1]) / 1000),
            "candles_count": len(candles)
        }

    def _normalize_cross_exchange_pressure(self, spot: dict, mexc_volume_ratio_30: float) -> dict:
        try:
            buy_ratio = float((spot or {}).get("buy_ratio") or 0.5)
            delta = float((spot or {}).get("delta_usdt") or 0.0)
            total = float((spot or {}).get("buy_usdt") or 0.0) + float((spot or {}).get("sell_usdt") or 0.0)
            delta_score = (delta / total) if total > 0 else 0.0
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

    async def _recent_btc_ai_stop_losses(self) -> int:
        """Return count of recent consecutive BTC AI stop-loss exits.

        This is a safety gate only. It must never crash the live/test cycle;
        if trade history cannot be read, return 0 and allow the scan to continue.
        """
        try:
            rows = await self.storage.trade_rows()
        except Exception as e:
            log_event("btc_ai_recent_sl_read_error", ok=False, error=str(e)[:500])
            return 0
        try:
            settings = await self.storage.all_settings()
            override_ts = float(settings.get("btc_ai_pause_manual_override_ts", 0) or 0)
        except Exception:
            override_ts = 0.0

        def _ts(row: dict) -> float:
            for key in ("ts_close", "closed_at", "ts", "updated_at"):
                try:
                    val = float(row.get(key) or 0)
                    if val > 0:
                        return val
                except Exception:
                    pass
            return 0.0

        def _is_btc_ai(row: dict) -> bool:
            try:
                strat = str(row.get("strategy") or "").lower()
                sym = str(row.get("symbol") or "").upper().replace("/", "_").replace(":USDT", "")
                return strat == "btc_ai_4h" and sym in {"BTC_USDT", "BTCUSDT"}
            except Exception:
                return False

        def _is_stop_loss(row: dict) -> bool:
            text = " ".join(str(row.get(k) or "") for k in ("reason", "result", "close_reason", "event", "type")).lower()
            try:
                pnl = float(row.get("pnl_usdt") or row.get("pnl") or 0)
            except Exception:
                pnl = 0.0
            stop_markers = ("stop", "stop_loss", "sl", "tpsl_sl", "bot_sl")
            return any(m in text for m in stop_markers) and pnl <= 0

        count = 0
        filtered = []
        for r in rows or []:
            if not isinstance(r, dict) or not _is_btc_ai(r):
                continue
            if override_ts > 0 and _ts(r) <= override_ts:
                continue
            filtered.append(r)
        for row in sorted(filtered, key=_ts, reverse=True):
            if _is_stop_loss(row):
                count += 1
                continue
            # Consecutive streak ends at the first non-stop BTC AI closed trade.
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

Return STRICT JSON only with numeric prices. Be conservative. If setup quality is weak, return WAIT and probability below 65. Use 65-74 only for marginal but tradable reduced-risk setups.

Trading rules:
- BTC only. Return WAIT if probability <65.
- You, the AI, must calculate entry_zone, stop_loss, take_profit_1 and take_profit_2 from 4H chart structure, liquidity, support/resistance, volatility, MEXC futures data and Binance spot confirmation.
- Do not use fixed +2%/+4% take-profits unless the visible market structure genuinely supports those exact levels.
- 65-74.9: reduced setup. Bot places LIMIT at the midpoint of your entry_zone, uses 5% balance, needs exactly one target: take_profit_1. Set take_profit_2=0. Bot closes 100% at your TP1.
- 75-84.9: normal setup. Bot places LIMIT at the midpoint of your entry_zone, uses 10% balance. Provide TP1 and TP2. Bot closes 50% at your TP1 and 50% at your TP2.
- 85+: A+ setup. Bot enters MARKET immediately with 10% balance. Provide TP1 and TP2 based on current market entry, not a distant pullback. Bot closes 50% at your TP1 and 50% at your TP2.
- Bot will NOT modify your take-profit levels. Bad/missing TP levels make the signal invalid.
- Bot may only widen stop_loss if it is closer than 1% from executable entry. If your required stop distance is above 2%, prefer WAIT instead of forcing the trade.
- LONG geometry: stop_loss < entry < take_profit_1 <= take_profit_2. For reduced 65-74.9: stop_loss < entry < take_profit_1 and take_profit_2=0.
- SHORT geometry: take_profit_2 <= take_profit_1 < entry < stop_loss. For reduced 65-74.9: take_profit_1 < entry < stop_loss and take_profit_2=0.
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

    def format_live_test_preview(self, d, md, lv=None, original_signal="WAIT", original_probability=0.0):
        lv = lv or {}
        entry_mid = float(lv.get("entry_mid") or 0)
        stop = float(lv.get("stop_loss") or 0)
        tp1 = float(lv.get("take_profit_1") or 0)
        tp2 = float(lv.get("take_profit_2") or 0)
        return (f"🧪 BTC AI LIVE TEST\n"
                f"ИИ ответил: {original_signal} ({float(original_probability or 0):.1f}%)\n"
                f"Тест принудительно откроет MARKET {d.signal}\n"
                f"Вход: ~{entry_mid:.2f}\n"
                f"SL: {stop:.2f}\n"
                f"TP1: {tp1:.2f} (ИИ, 50%)\n"
                f"TP2: {tp2:.2f} (ИИ, остаток)\n"
                f"График для ИИ: чистый. Этот график: с уровнями.")

    def format_decision(self,d,md,lv=None):
        if d.error: return f"❌ BTC AI error: {d.error}"
        lv = lv or {}
        entry_mid = float(lv.get("entry_mid") or 0)
        stop = float(lv.get("stop_loss") or 0)
        tp1 = float(lv.get("take_profit_1") or 0)
        tp2 = float(lv.get("take_profit_2") or 0)
        reduced = bool(lv.get("reduced_mode")) or (65.0 <= float(d.probability or 0) < 75.0)
        tp_text = (f"TP: {tp1:.2f} (ИИ, закрыть 100%)") if reduced else (f"TP1: {tp1:.2f} (ИИ, 50%)\nTP2: {tp2:.2f} (ИИ, остаток)")
        stop_note = " · SL расширен ботом до мин. 1%" if bool(lv.get("stop_adjusted_by_bot")) else ""
        return (f"✅ BTC AI 4H сигнал: {d.signal}\n"
                f"Проходимость ИИ: {float(d.probability or 0):.1f}%\n"
                f"Вход: {entry_mid:.2f}\n"
                f"Enter zone: {float(lv.get('entry_low') or d.entry_zone_low):.2f}-{float(lv.get('entry_high') or d.entry_zone_high):.2f}\n"
                f"SL: {stop:.2f} ({float(lv.get('stop_pct') or 0):.2f}%){stop_note}\n"
                f"{tp_text}\n"
                f"Свеча 4H закрыта: {md.get('closed_candle_msk')}\n"
                f"Виртуальное сопровождение: ВКЛ")

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
