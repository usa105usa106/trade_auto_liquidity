import time
import os
from debug_log import log_event
from chatgpt_runtime_logger import chatgpt_log_event
from scalp_exit_engine import ScalpExitPolicy
from protection_engine import ProtectionEngine

class PositionManager:
    """
    Manages open and pending positions.

    Hardened position lifecycle:
    - pending limit timeout/cancel lifecycle
    - pending limit fill detection is based on fetch_order status, not disappearance
    - position management is independent from new-entry gate
    - breakeven, TP, SL, time-stop continue on REST fallback
    """

    def __init__(self, storage, execution_engine):
        self.storage = storage
        self.execution_engine = execution_engine
        self.time_stop_sec = int(os.getenv("TIME_STOP_SEC", "300"))
        self.limit_timeout_sec = int(os.getenv("LIMIT_TIMEOUT_SEC", "300"))
        self.breakeven_trigger_pct = float(os.getenv("BREAKEVEN_TRIGGER_PCT", "0.12"))
        self.scalp_exit_policy = ScalpExitPolicy()

    @staticmethod
    def _truthy(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    async def _setting(self, key: str, default):
        try:
            if hasattr(self.storage, "get"):
                value = await self.storage.get(key, None)
                if value is not None:
                    return value
        except Exception:
            pass
        return default

    async def _runtime_limits(self) -> tuple[int, int, float]:
        time_stop = int(await self._setting("time_stop_sec", self.time_stop_sec) or self.time_stop_sec)
        limit_timeout = int(await self._setting("limit_timeout_sec", self.limit_timeout_sec) or self.limit_timeout_sec)
        breakeven = float(await self._setting("breakeven_trigger_pct", self.breakeven_trigger_pct) or self.breakeven_trigger_pct)
        return time_stop, limit_timeout, breakeven

    async def _refresh_scalp_policy(self) -> ScalpExitPolicy:
        policy = ScalpExitPolicy()
        policy.enabled = self._truthy(await self._setting("scalp_exit_enabled", policy.enabled), policy.enabled)
        policy.breakeven_trigger_pct = float(await self._setting("breakeven_trigger_pct", policy.breakeven_trigger_pct) or policy.breakeven_trigger_pct)
        policy.breakeven_offset_pct = float(await self._setting("breakeven_offset_pct", policy.breakeven_offset_pct) or policy.breakeven_offset_pct)
        policy.trailing_enabled = self._truthy(await self._setting("scalp_trailing_enabled", policy.trailing_enabled), policy.trailing_enabled)
        policy.trailing_start_pct = float(await self._setting("scalp_trailing_start_pct", policy.trailing_start_pct) or policy.trailing_start_pct)
        policy.trailing_giveback_pct = float(await self._setting("scalp_trailing_giveback_pct", policy.trailing_giveback_pct) or policy.trailing_giveback_pct)
        policy.time_min_sec = int(await self._setting("smart_time_stop_min_sec", policy.time_min_sec) or policy.time_min_sec)
        policy.stale_pnl_abs_pct = float(await self._setting("smart_time_stop_stale_abs_pct", policy.stale_pnl_abs_pct) or policy.stale_pnl_abs_pct)
        policy.time_extend_profit_pct = float(await self._setting("smart_time_stop_extend_profit_pct", policy.time_extend_profit_pct) or policy.time_extend_profit_pct)
        policy.time_max_extend_sec = int(await self._setting("smart_time_stop_max_extend_sec", policy.time_max_extend_sec) or policy.time_max_extend_sec)
        self.scalp_exit_policy = policy
        return policy



    async def _liquidity_runner_enabled(self) -> bool:
        value = await self._setting("liquidity_runner_enabled", os.getenv("LIQUIDITY_RUNNER_ENABLED", "false"))
        return self._truthy(value, False)

    async def _liquidity_runner_stop(self, pos: dict, price: float, current_stop: float) -> tuple[bool, float, int]:
        """Progressively lock profit for liquidity_retest positions.

        This is not scalp trailing. It moves the local stop by structure/R steps:
        - 1R: existing breakeven logic protects entry.
        - 2R: lock +1R.
        - 3R: lock +2R.
        - 4R+: lock +3R if TP/liquidity target is farther.

        The feature is controlled by liquidity_runner_enabled and only applies
        to strategy=liquidity_retest. It never loosens a stop and never moves
        the stop past current price.
        """
        if not await self._liquidity_runner_enabled():
            return False, 0.0, int(pos.get("liquidity_runner_stage") or 0)
        side = str(pos.get("side") or "").upper()
        entry = float(pos.get("entry_price") or 0)
        initial_stop = float(pos.get("initial_stop_price") or pos.get("original_stop_price") or 0)
        if entry <= 0 or price <= 0:
            return False, 0.0, int(pos.get("liquidity_runner_stage") or 0)
        if initial_stop <= 0:
            initial_stop = float(pos.get("stop_price") or 0)
        risk_abs = abs(entry - initial_stop)
        if risk_abs <= 0:
            return False, 0.0, int(pos.get("liquidity_runner_stage") or 0)
        if side == "LONG":
            r_now = (price - entry) / risk_abs
        elif side == "SHORT":
            r_now = (entry - price) / risk_abs
        else:
            return False, 0.0, int(pos.get("liquidity_runner_stage") or 0)

        target_stage = 0
        if r_now >= 4.0:
            target_stage = 4
        elif r_now >= 3.0:
            target_stage = 3
        elif r_now >= 2.0:
            target_stage = 2
        else:
            return False, 0.0, int(pos.get("liquidity_runner_stage") or 0)

        current_stage = int(pos.get("liquidity_runner_stage") or 0)
        if target_stage <= current_stage:
            return False, 0.0, current_stage

        # Lock one R behind the reached stage: 2R -> +1R, 3R -> +2R, 4R -> +3R.
        lock_r = max(1, target_stage - 1)
        if side == "LONG":
            new_stop = entry + lock_r * risk_abs
            if current_stop and new_stop <= current_stop:
                return False, 0.0, current_stage
            if new_stop >= price:
                return False, 0.0, current_stage
        else:
            new_stop = entry - lock_r * risk_abs
            if current_stop and new_stop >= current_stop:
                return False, 0.0, current_stage
            if new_stop <= price:
                return False, 0.0, current_stage
        return True, float(new_stop), target_stage


    async def _live_boost_profit_confirmed(self, pos: dict, local_pnl_pct: float, min_profit_pct: float = 0.0) -> tuple[bool, str]:
        """For LIVE BOOST tiny TP, confirm profit using exchange mark/uPnL.

        Local REST/WS prices can say +0.03% while the real MEXC position is
        negative after spread/slippage.  Do not close BOOST take-profit unless
        exchange mark PnL and/or unrealizedPnl confirms it.
        """
        if str(pos.get("strategy") or "").lower() != "boost_scalping":
            return True, "not_boost"
        try:
            row = await self.execution_engine._find_exchange_position_row(pos.get("symbol"))
            if not row:
                return True, "no_exchange_row"
            info = row.get("info") or {}
            upnl = None
            for k in ("unrealizedPnl", "unrealised", "profit"):
                try:
                    v = row.get(k) if row.get(k) not in (None, "") else (info.get(k) if isinstance(info, dict) else None)
                    if v not in (None, ""):
                        upnl = float(v); break
                except Exception:
                    pass
            entry = float(pos.get("entry_price") or 0)
            try:
                if entry <= 0 and row.get("entryPrice") not in (None, ""):
                    entry = float(row.get("entryPrice"))
            except Exception:
                pass
            if entry <= 0 and isinstance(info, dict):
                for k in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
                    try:
                        if info.get(k) not in (None, ""):
                            entry = float(info.get(k)); break
                    except Exception:
                        pass
            mark = 0.0
            for k in ("markPrice", "fairPrice", "lastPrice"):
                try:
                    v = row.get(k) if row.get(k) not in (None, "") else (info.get(k) if isinstance(info, dict) else None)
                    if v not in (None, ""):
                        mark = float(v); break
                except Exception:
                    pass
            ex_pct = local_pnl_pct
            if entry > 0 and mark > 0:
                if str(pos.get("side") or "").upper() == "LONG":
                    ex_pct = (mark - entry) / entry * 100.0
                else:
                    ex_pct = (entry - mark) / entry * 100.0
            ok = ex_pct >= min_profit_pct and (upnl is None or upnl > 0)
            return bool(ok), f"exchange_pct={ex_pct:+.4f}% local_pct={local_pnl_pct:+.4f}% uPnL={(upnl if upnl is not None else 0):+.5f}"
        except Exception as e:
            return False, f"exchange_confirm_error={str(e)[:120]}"

    async def _is_terminal_closed(self, symbol: str) -> bool:
        """Return True when a recent close lock makes local callbacks stale.

        MEXC can report stale margin/order state for a few seconds after flat.
        Once ExecutionEngine closes a symbol, CLOSED is terminal for local TP/SL,
        breakeven and time-stop callbacks until the cooldown lock expires.
        """
        try:
            locked, reason = await self.storage.is_locked(symbol)
            return bool(locked and str(reason or "").startswith("closed:"))
        except Exception:
            return False

    async def _close_and_event(self, pos: dict, event_type: str, reason: str, live: bool, price: float) -> dict | None:
        symbol = pos["symbol"]
        if await self._is_terminal_closed(symbol):
            try:
                await self.storage.remove_position(symbol)
            except Exception:
                pass
            return None
        res = await self.execution_engine.close_position(pos, reason, live, price)
        try:
            strategy_name = str(pos.get("strategy", "")).lower()
            if strategy_name in {"quick_bounce", "impulse_dump"}:
                log_event(
                    f"{strategy_name}_closed",
                    stage="exit",
                    ok=bool(isinstance(res, dict) and res.get("ok")),
                    symbol=str(symbol),
                    side=str(pos.get("side", "")),
                    reason=str(reason),
                    event_type=str(event_type),
                    exit_price=float(price or 0),
                    result=res,
                )
        except Exception:
            pass
        if isinstance(res, dict) and res.get("ok"):
            # Close is terminal locally. Remove any stale local row again and rely
            # on /positions or sync to restore it only if exchange still has a
            # real position after settlement.
            try:
                await self.storage.remove_position(symbol)
            except Exception:
                pass
        return {"type": event_type, "symbol": symbol, "result": res}


    async def _protection_watchdog(self, pos: dict, live: bool) -> dict | None:
        """Periodically re-check and reattach exchange TP/SL for open positions.

        This is intentionally independent from new-entry pause/run gates. After
        a restart the bot may have local rows restored from MEXC, but MEXC TP/SL
        legs may be missing. The watchdog keeps monitoring locally and also
        keeps trying to rebuild exchange protection.
        """
        if not live:
            return None
        now = time.time()
        interval = int(await self._setting("protection_watchdog_sec", os.getenv("PROTECTION_WATCHDOG_SEC", "20")) or 20)
        try:
            if interval <= 0 or now - float(pos.get("protection_checked_at") or pos.get("checked_at") or 0) < interval:
                return None
        except Exception:
            pass
        try:
            pe = ProtectionEngine(self.execution_engine.exchange_client, self.execution_engine)
            state = await pe.reconcile(pos, live=True, reattach=True)
            pos.update(state)
            pos["protection_checked_at"] = now
            # v0165: native MEXC stoporder rows expose tp_exists/sl_exists;
            # mirror them to notification fields and do not downgrade a valid
            # exchange/native protection row to local monitoring.
            state.setdefault("take_profit_ok", bool(state.get("tp_exists")))
            state.setdefault("stop_loss_ok", bool(state.get("sl_exists")))
            strategy_name = str(pos.get("strategy") or "").lower()
            protected = state.get("protection_status") in {"EXCHANGE PROTECTED", "TP + LIQUIDATION STOP", "LOCAL_FAST_PROTECTED", "EMERGENCY SL ONLY", "VIRTUAL_PROTECTED"}
            boost_safe = strategy_name == "boost_scalping" and str(await self._setting("boost_live_safe_execution", os.getenv("BOOST_LIVE_SAFE_EXECUTION", "true"))).lower() in {"1", "true", "yes", "on"}
            unsafe_boost = strategy_name == "boost_scalping" and (state.get("protection_status") == "UNSAFE POSITION" or state.get("boost_unsafe_position"))
            if not protected and boost_safe and not unsafe_boost:
                # BOOST local fast monitoring is accepted only when we are NOT in
                # emergency-SL-only/UNSAFE state.  Do not overwrite UNSAFE or
                # EMERGENCY SL ONLY with local_fast, because that made the next
                # watchdog pass forget the planorder mode and call cancel_all.
                state["protection_status"] = "LOCAL_FAST_PROTECTED"
                state["protection_mode"] = "local_fast"
                state["protection_note"] = "BOOST local fast monitor active; watchdog will retry exchange protection"
                pos["protection_mode"] = "local_fast"
                pos.pop("protection_warning", None)
                protected = True
            elif not protected:
                pos["protection_mode"] = "local_monitoring"
                pos["protection_warning"] = "exchange TP/SL not confirmed; bot monitors TP/SL locally"
            else:
                pos["protection_mode"] = state.get("protection_mode") or "exchange"
                if state.get("protection_status") == "EMERGENCY SL ONLY":
                    pos["boost_emergency_sl_only"] = True
                    pos["tp_order_id"] = pos.get("tp_order_id") or "LIVE_TRAILING_NO_FIXED_TP"
                    pos["boost_unsafe_position"] = False
                    pos["boost_defensive_mode"] = False
                pos.pop("protection_warning", None)
            await self.storage.upsert_position(pos)
            if state.get("reattach_attempted") or not protected:
                return {"type": "protection_watchdog", "symbol": pos.get("symbol"), **state}
        except Exception as e:
            pos["protection_warning"] = f"protection watchdog error: {str(e)[:180]}"
            pos["protection_checked_at"] = now
            await self.storage.upsert_position(pos)
            return {"type": "protection_watchdog_error", "symbol": pos.get("symbol"), "error": str(e)}
        return None

    async def _auto_close_on_protection_failed(self) -> bool:
        value = await self._setting("auto_close_on_protection_failed", os.getenv("AUTO_CLOSE_ON_PROTECTION_FAILED", os.getenv("ALLOW_AUTO_CLOSE_ON_PROTECTION_FAILED", "false")))
        return self._truthy(value, False)

    def pnl_pct(self, pos, price):
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return 0.0
        return (price-entry)/entry*100 if str(pos.get("side")).upper()=="LONG" else (entry-price)/entry*100



    def _position_symbol_matches(self, row: dict, symbol: str) -> bool:
        try:
            return bool(self.execution_engine._position_symbol_matches(row, symbol))
        except Exception:
            row_symbol = str(row.get("symbol") or row.get("mexc_symbol") or "")
            return row_symbol.replace(":USDT", "").replace("/", "_").upper() == str(symbol or "").replace("/", "_").upper()

    def _exchange_row_contracts(self, row: dict) -> float:
        try:
            return float(self.execution_engine._exchange_row_contracts(row))
        except Exception:
            info = row.get("info", {}) if isinstance(row.get("info"), dict) else {}
            for key in ("contracts", "qty", "amount", "size"):
                try:
                    v = row.get(key)
                    if v not in (None, ""):
                        return abs(float(v))
                except Exception:
                    pass
            for key in ("holdVol", "vol", "positionAmt"):
                try:
                    v = info.get(key)
                    if v not in (None, ""):
                        return abs(float(v))
                except Exception:
                    pass
            return 0.0

    async def _find_exchange_position_row(self, symbol: str) -> dict | None:
        try:
            if hasattr(self.execution_engine, "_find_exchange_position_row"):
                row = await self.execution_engine._find_exchange_position_row(symbol)
                if row:
                    return row
        except Exception:
            pass
        try:
            rows = await self.execution_engine.exchange_client.fetch_positions([symbol])
        except Exception:
            rows = []
        for row in rows or []:
            if self._position_symbol_matches(row, symbol) and self._exchange_row_contracts(row) > 0:
                return row
        return None

    async def _chatgpt_move_sl_to_be_after_tp1(self, pos: dict, live: bool) -> dict | None:
        """For ChatGPT setup positions: after TP1 partially closes on MEXC,
        cancel the old full-size SL and place a new SL at entry for the remaining position.
        No trailing/scalp logic is used.
        """
        if not live:
            return None
        if str(pos.get("strategy") or "").lower() != "chatgpt_setup":
            return None
        if pos.get("chatgpt_tp1_breakeven_done") or pos.get("breakeven_moved"):
            return None
        details = pos.get("signal_details") if isinstance(pos.get("signal_details"), dict) else {}
        mgmt = details.get("trade_management") if isinstance(details.get("trade_management"), dict) else {}
        if not self._truthy(mgmt.get("breakeven_after_tp1_only", pos.get("breakeven_after_tp1_only", True)), True):
            return None
        tps = details.get("chatgpt_take_profits") or []
        if not isinstance(tps, list) or not tps:
            return None
        try:
            tp1_size_pct = float((tps[0] or {}).get("size_percent") or 0)
        except Exception:
            tp1_size_pct = 0.0
        if tp1_size_pct <= 0:
            return None
        symbol = pos.get("symbol")
        entry = float(pos.get("entry_price") or 0)
        side = str(pos.get("side") or "").upper()
        if not symbol or entry <= 0 or side not in {"LONG", "SHORT"}:
            return None

        row = await self._find_exchange_position_row(symbol)
        if not row:
            chatgpt_log_event("tp1_be_check_no_exchange_position", symbol=symbol)
            return None
        cur_qty = self._exchange_row_contracts(row)
        chatgpt_log_event("tp1_be_check", symbol=symbol, current_qty=cur_qty, local_qty=pos.get("qty"), original_qty=pos.get("chatgpt_original_qty"), entry=entry)
        if cur_qty <= 0:
            return None
        orig_qty = float(pos.get("chatgpt_original_qty") or pos.get("qty") or 0)
        if orig_qty <= 0:
            orig_qty = cur_qty
            pos["chatgpt_original_qty"] = cur_qty
            await self.storage.upsert_position(pos)
            chatgpt_log_event("tp1_be_original_qty_initialized", symbol=symbol, original_qty=cur_qty)
            return None
        # Detect TP1 by real exchange size reduction. Use 75% of expected TP1
        # so rounding/contract-size differences do not block BE forever.
        expected_reduction = orig_qty * min(max(tp1_size_pct, 0.0), 100.0) / 100.0
        actual_reduction = orig_qty - cur_qty
        if expected_reduction <= 0 or actual_reduction < expected_reduction * 0.75:
            chatgpt_log_event("tp1_be_not_yet", symbol=symbol, original_qty=orig_qty, current_qty=cur_qty, expected_reduction=expected_reduction, actual_reduction=actual_reduction)
            return None

        chatgpt_log_event("tp1_hit_detected", symbol=symbol, original_qty=orig_qty, current_qty=cur_qty, expected_reduction=expected_reduction, actual_reduction=actual_reduction)
        old_sl_id = str(pos.get("sl_order_id") or "")
        if old_sl_id:
            try:
                if hasattr(self.execution_engine.exchange_client, "mexc_cancel_plan_order"):
                    cancel_res = await self.execution_engine.exchange_client.mexc_cancel_plan_order(symbol, old_sl_id)
                else:
                    cancel_res = await self.execution_engine.exchange_client.cancel_order(old_sl_id, symbol)
                log_event("chatgpt_tp1_cancel_old_sl", symbol=symbol, old_sl_order_id=old_sl_id, result=cancel_res, ok=True)
                chatgpt_log_event("breakeven_cancel_old_sl_ok", symbol=symbol, old_sl_order_id=old_sl_id, result=cancel_res)
            except Exception as e:
                # Do not place a duplicate SL if old full-size SL could not be canceled.
                pos["chatgpt_tp1_be_cancel_sl_error"] = str(e)[:240]
                pos["updated_at"] = time.time()
                await self.storage.upsert_position(pos)
                log_event("chatgpt_tp1_cancel_old_sl_error", symbol=symbol, old_sl_order_id=old_sl_id, error=str(e), ok=False)
                chatgpt_log_event("breakeven_cancel_old_sl_error", symbol=symbol, old_sl_order_id=old_sl_id, error=str(e))
                return {"type": "chatgpt_tp1_be_wait_cancel_sl", "symbol": symbol, "error": str(e)[:160]}
        try:
            chatgpt_log_event("breakeven_new_sl_start", symbol=symbol, qty=cur_qty, stop_price=entry)
            order = await self.execution_engine._create_stop_market_order(symbol, side, cur_qty, entry)
        except Exception as e:
            pos["chatgpt_tp1_be_new_sl_error"] = str(e)[:240]
            pos["updated_at"] = time.time()
            await self.storage.upsert_position(pos)
            log_event("chatgpt_tp1_new_be_sl_error", symbol=symbol, qty=cur_qty, entry=entry, error=str(e), ok=False)
            chatgpt_log_event("breakeven_new_sl_error", symbol=symbol, qty=cur_qty, entry=entry, error=str(e))
            return {"type": "chatgpt_tp1_be_wait_new_sl", "symbol": symbol, "error": str(e)[:160]}
        pos["qty"] = cur_qty
        pos["stop_price"] = entry
        pos["sl_order_id"] = order.get("id")
        pos["sl_raw"] = order
        pos["breakeven_moved"] = True
        pos["chatgpt_tp1_breakeven_done"] = True
        pos["chatgpt_tp1_be_qty"] = cur_qty
        pos["chatgpt_tp1_be_price"] = entry
        pos["updated_at"] = time.time()
        pos["protection_checked_at"] = time.time()
        await self.storage.upsert_position(pos)
        log_event("chatgpt_tp1_breakeven_moved", symbol=symbol, qty=cur_qty, stop_price=entry, old_sl_order_id=old_sl_id, new_sl_order_id=order.get("id"), ok=True)
        chatgpt_log_event("breakeven_move_ok", symbol=symbol, qty=cur_qty, stop_price=entry, old_sl_order_id=old_sl_id, new_sl_order_id=order.get("id"), order=order)
        return {"type": "chatgpt_tp1_breakeven", "symbol": symbol, "stop_price": entry, "qty": cur_qty}

    async def _manage_pending(self, pos: dict, live: bool) -> dict | None:
        now = time.time()
        opened = float(pos.get("opened_at") or now)
        symbol = pos["symbol"]
        strategy = str(pos.get("strategy") or "").lower()
        is_chatgpt_setup = strategy == "chatgpt_setup"
        # ChatGPT setup mode places REAL MEXC limit orders even when the global
        # live_trading toggle was turned off to disable other strategies.  Never
        # simulate these as paper fills.  Keep them PENDING until MEXC confirms
        # the order is filled or a real exchange position exists.
        if is_chatgpt_setup:
            live = True
            # v23: pending ChatGPT LIMITs were being polled every execution loop
            # tick (~0.25s), which spammed /log_chatgpt and could hit MEXC rate
            # limits. Check each pending order only once per configurable interval.
            try:
                check_interval = float(os.getenv("CHATGPT_PENDING_CHECK_INTERVAL_SEC", "10") or 10)
            except Exception:
                check_interval = 10.0
            try:
                last_check = float(pos.get("chatgpt_pending_last_check_at") or 0)
            except Exception:
                last_check = 0.0
            if last_check > 0 and now - last_check < max(1.0, check_interval):
                return None
            pos["chatgpt_pending_last_check_at"] = now
            pos["updated_at"] = now
            try:
                await self.storage.upsert_position(pos)
            except Exception:
                pass
            chatgpt_log_event("pending_limit_check", symbol=symbol, status=pos.get("status"), order_id=pos.get("order_id"), opened_at=pos.get("opened_at"), entry=pos.get("entry_price"), stop=pos.get("stop_price"), take=pos.get("take_price"), check_interval_sec=check_interval)
        if not live:
            # Paper mode simulation is allowed only for non-ChatGPT strategies.
            pos["status"] = "open"
            pos["updated_at"] = now
            pos = self.execution_engine._decorate_position_metrics(pos)
            await self.storage.upsert_position(pos)
            if is_chatgpt_setup:
                chatgpt_log_event("ERROR_chatgpt_paper_fill_attempt_blocked", symbol=symbol)
            return {"type": "paper_limit_filled", "symbol": symbol}

        # Timeout: cancel stale limit entry and free slot.
        _time_stop_sec, limit_timeout_sec, _breakeven_trigger_pct = await self._runtime_limits()
        details = pos.get("signal_details") if isinstance(pos.get("signal_details"), dict) else {}
        try:
            custom_minutes = int(float(details.get("cancel_if_not_filled_minutes") or 0))
            if custom_minutes > 0:
                limit_timeout_sec = custom_minutes * 60
        except Exception:
            pass
        if now - opened >= limit_timeout_sec:
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_timeout_cancel_start", symbol=symbol, age_sec=round(now-opened, 2), timeout_sec=limit_timeout_sec, order_id=pos.get("order_id"))
            res = await self.execution_engine.cancel_entry(pos, live=True, reason="limit_timeout")
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_timeout_cancel_done", symbol=symbol, result=res)
            return {"type": "limit_timeout", "symbol": symbol, "result": res}

        # ChatGPT LIMIT safety: if price already reached TP1 before entry fill,
        # the idea is stale; cancel the pending order. Optional stop-before-entry
        # cancellation is also supported if setup.txt includes it.
        try:
            tps = details.get("chatgpt_take_profits") or []
            tp1 = float((tps[0] or {}).get("price") or 0) if isinstance(tps, list) and tps else 0.0
            sl = float(pos.get("stop_price") or 0)
            side_u = str(pos.get("side") or "").upper()
            cancel_tp1 = str(details.get("cancel_if_tp1_before_entry") or "").lower() in {"1", "true", "yes", "on"}
            cancel_sl = str(details.get("cancel_if_stop_before_entry") or "").lower() in {"1", "true", "yes", "on"}
            if (cancel_tp1 and tp1 > 0) or (cancel_sl and sl > 0):
                ticker = await self.execution_engine.exchange_client.fetch_ticker(symbol)
                cur = float(ticker.get("last") or ticker.get("close") or 0)
                if cur > 0:
                    if cancel_tp1 and tp1 > 0 and ((side_u == "LONG" and cur >= tp1) or (side_u == "SHORT" and cur <= tp1)):
                        if is_chatgpt_setup:
                            chatgpt_log_event("pending_limit_stale_tp1_before_entry", symbol=symbol, current_price=cur, tp1=tp1, order_id=pos.get("order_id"))
                        res = await self.execution_engine.cancel_entry(pos, live=True, reason="tp1_touched_before_entry")
                        if is_chatgpt_setup:
                            chatgpt_log_event("pending_limit_stale_tp1_cancel_done", symbol=symbol, result=res)
                        return {"type": "limit_stale_tp1_before_entry", "symbol": symbol, "result": res}
                    if cancel_sl and sl > 0 and ((side_u == "LONG" and cur <= sl) or (side_u == "SHORT" and cur >= sl)):
                        if is_chatgpt_setup:
                            chatgpt_log_event("pending_limit_stale_stop_before_entry", symbol=symbol, current_price=cur, stop=sl, order_id=pos.get("order_id"))
                        res = await self.execution_engine.cancel_entry(pos, live=True, reason="stop_touched_before_entry")
                        if is_chatgpt_setup:
                            chatgpt_log_event("pending_limit_stale_stop_cancel_done", symbol=symbol, result=res)
                        return {"type": "limit_stale_stop_before_entry", "symbol": symbol, "result": res}
        except Exception:
            pass

        # Confirm the exact order state. Disappearance from open orders is not
        # enough: it may mean filled, canceled, rejected, or expired.
        oid = str(pos.get("order_id") or "")
        if not oid:
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_sync_warning", symbol=symbol, error="missing order_id")
            return {"type": "pending_sync_warning", "symbol": symbol, "error": "missing order_id"}
        try:
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_fetch_order_start", symbol=symbol, order_id=oid)
            order = await self.execution_engine.exchange_client.fetch_order(oid, symbol)
            status = str(order.get("status") or "").lower()
            filled = float(order.get("filled") or 0)
            amount = float(order.get("amount") or pos.get("qty") or 0)
            avg = float(order.get("average") or order.get("price") or pos.get("entry_price") or 0)
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_fetch_order_result", symbol=symbol, order_id=oid, status=status, filled=filled, amount=amount, average=avg, raw=order)
            if status in {"closed", "filled"} or (amount > 0 and filled >= amount * 0.999):
                pos["status"] = "open"
                pos["entry_price"] = avg or pos.get("entry_price")
                if filled > 0:
                    pos["qty"] = filled
                    pos.setdefault("chatgpt_original_qty", filled)
                pos["updated_at"] = now
                pos = self.execution_engine._decorate_position_metrics(pos)
                if is_chatgpt_setup:
                    chatgpt_log_event("limit_filled_confirmed", symbol=symbol, order_id=oid, filled=filled, amount=amount, entry_price=pos.get("entry_price"), qty=pos.get("qty"))
                    chatgpt_log_event("sl_tp_place_start", symbol=symbol, qty=pos.get("qty"), stop=pos.get("stop_price"), take=pos.get("take_price"), tps=details.get("chatgpt_take_profits"))
                protection = await self.execution_engine.place_protection_orders(pos, live=True)
                if is_chatgpt_setup:
                    chatgpt_log_event("sl_tp_place_result", symbol=symbol, ok=bool(protection.get("ok")), protection=protection)
                pos.update(protection)
                await self.storage.upsert_position(pos)
                if not protection.get("ok"):
                    # v0066: keep the local position and let PositionManager
                    # enforce TP/SL/time-stop from ticker prices. Auto-closing
                    # can be enabled explicitly, but default is safer state sync.
                    pos["protection_mode"] = "local_monitoring"
                    pos["protection_warning"] = "exchange protection failed; bot monitors TP/SL locally"
                    pos.update(protection)
                    await self.storage.upsert_position(pos)
                    # Never kill an already-filled live position only because
                    # TP/SL attachment failed. Local monitoring below still closes
                    # on real take_price/stop_price, and the watchdog keeps trying
                    # to reattach exchange protection.
                    pos["auto_close_on_protection_failed_ignored"] = True
                    await self.storage.upsert_position(pos)
                    if is_chatgpt_setup:
                        chatgpt_log_event("sl_tp_protection_failed_local_monitoring", symbol=symbol, protection=protection)
                    return {"type": "protection_local", "symbol": symbol, "protection": protection}
                if is_chatgpt_setup:
                    chatgpt_log_event("limit_position_open_ready", symbol=symbol, status="open", protection_mode=protection.get("protection_mode"), sl_order_id=protection.get("sl_order_id"), tp1_order_id=protection.get("tp1_order_id"), tp2_order_id=protection.get("tp2_order_id"), tp3_order_id=protection.get("tp3_order_id"))
                return {"type": "limit_filled", "symbol": symbol}
            if status in {"canceled", "cancelled", "rejected", "expired"}:
                if is_chatgpt_setup:
                    chatgpt_log_event("pending_limit_terminal_status", symbol=symbol, order_id=oid, status=status, order=order)
                await self.storage.remove_position(symbol)
                await self.storage.set_lock(symbol, 30, f"limit_{status}")
                return {"type": f"limit_{status}", "symbol": symbol}
        except Exception as e:
            # On sync error keep pending but don't duplicate entries; occupied slot remains used.
            if is_chatgpt_setup:
                chatgpt_log_event("pending_limit_sync_error", symbol=symbol, order_id=oid, error=str(e))
            return {"type": "pending_sync_warning", "symbol": symbol, "error": str(e)}
        return None

    async def manage(self, price_provider, live: bool):
        events=[]; now=time.time()
        time_stop_sec, _limit_timeout_sec, breakeven_trigger_pct = await self._runtime_limits()
        for pos in await self.storage.positions():
            status = pos.get("status")
            if status == "pending":
                ev = await self._manage_pending(pos, live)
                if ev:
                    events.append(ev)
                continue
            if status not in {"open"}:
                continue
            symbol=pos["symbol"]
            strategy = str(pos.get("strategy") or "").lower()
            is_chatgpt_setup = strategy == "chatgpt_setup"
            if is_chatgpt_setup:
                # ChatGPT setup positions must stay live-managed even if old entry modes are paused.
                live = True
            if await self._is_terminal_closed(symbol):
                # Ignore stale post-close callbacks such as breakeven moved after
                # SL/TP/time-stop. CLOSED is terminal.
                try:
                    await self.storage.remove_position(symbol)
                except Exception:
                    pass
                continue
            wd = await self._protection_watchdog(pos, live)
            if wd:
                events.append(wd)
            if is_chatgpt_setup:
                be_ev = await self._chatgpt_move_sl_to_be_after_tp1(pos, True)
                if be_ev:
                    events.append(be_ev)
                    # reload local fields after BE update
                    try:
                        for _p in await self.storage.positions():
                            if str(_p.get("symbol") or "") == str(symbol):
                                pos = _p
                                break
                    except Exception:
                        pass
            try:
                price=await price_provider(symbol)
            except Exception as e:
                events.append({"type":"price_error","symbol":symbol,"error":str(e)})
                continue
            if not price:
                continue
            side=str(pos.get("side")).upper(); stop=float(pos.get("stop_price") or 0); take=float(pos.get("take_price") or 0); entry=float(pos.get("entry_price") or 0); opened=float(pos.get("opened_at") or now); pnl=self.pnl_pct(pos, price)
            is_liquidity_retest = strategy == "liquidity_retest"
            is_ai_scalping = strategy == "ai_scalping"
            is_boost_scalping = strategy == "boost_scalping"
            is_impulse_dump = strategy == "impulse_dump"
            is_orderflow_impulse = strategy == "orderflow_impulse"
            is_cascade_hunter = strategy == "cascade_hunter"
            is_strongest_coin = strategy == "strongest_coin"
            is_knife_reversal = strategy == "knife_reversal"
            liquidation_stop_mode = bool(pos.get("liquidation_stop_mode")) and is_ai_scalping
            ai_manage_only_tpsl = str(await self._setting("ai_scalping_manage_only_tpsl", os.getenv("AI_SCALPING_MANAGE_ONLY_TPSL", "1"))).lower() in {"1", "true", "yes", "on"}
            boost_manage_only_tpsl = str(await self._setting("boost_manage_only_tpsl", os.getenv("BOOST_MANAGE_ONLY_TPSL", "1"))).lower() in {"1", "true", "yes", "on"}
            # v0240: impulse_dump must be closed only by its explicit PRICE
            # targets (exchange/local TP and SL), 24h hard timeout, or manual
            # close. Do not apply generic scalp ROI/trailing/smart time-stop or
            # breakeven moves; those can close after a leveraged PnL percentage
            # before the price reaches the calculated dump target.
            impulse_manage_only_tpsl = str(await self._setting("impulse_dump_manage_only_tpsl", os.getenv("IMPULSE_DUMP_MANAGE_ONLY_TPSL", "1"))).lower() in {"1", "true", "yes", "on"}
            orderflow_manage_only_tpsl = str(await self._setting("orderflow_impulse_manage_only_tpsl", os.getenv("ORDERFLOW_IMPULSE_MANAGE_ONLY_TPSL", "1"))).lower() in {"1", "true", "yes", "on"}
            # ChatGPT setup mode is managed ONLY by explicit SL and TP orders
            # from setup.txt.  Do not apply generic scalp breakeven, trailing,
            # smart time-stop, or local fake-profit exits.
            manage_only_tpsl = (is_ai_scalping and ai_manage_only_tpsl) or (is_boost_scalping and boost_manage_only_tpsl) or (is_impulse_dump and impulse_manage_only_tpsl) or ((is_orderflow_impulse or is_cascade_hunter or is_strongest_coin) and orderflow_manage_only_tpsl) or is_knife_reversal or is_chatgpt_setup
            policy = await self._refresh_scalp_policy()
            if not is_chatgpt_setup:
                policy.update_best_pnl(pos, pnl)
            if manage_only_tpsl:
                move_be, new_stop = False, 0.0
            elif is_liquidity_retest:
                # v0082: no aggressive scalp BE. Move to BE only after price has
                # travelled roughly 1R, because this strategy targets 2R-4R.
                risk_pct = 0.0
                if entry > 0 and stop > 0:
                    risk_pct = abs(entry - stop) / entry * 100.0
                be_trigger = max(float(await self._setting("liquidity_retest_be_r_pct", 1.0) or 1.0) * risk_pct, 0.20)
                move_be, new_stop = False, 0.0
                if pnl >= be_trigger and entry > 0:
                    if (side == "LONG" and (not stop or stop < entry)) or (side == "SHORT" and (not stop or stop > entry)):
                        move_be, new_stop = True, entry
            else:
                move_be, _pnl, new_stop = policy.should_move_breakeven(pos, price)
                # Backward-compatible fallback for users who disable the new policy.
                if not move_be and pnl>=breakeven_trigger_pct and entry>0:
                    if (side=="LONG" and stop<entry) or (side=="SHORT" and stop>entry):
                        move_be, new_stop = True, entry
            if move_be and entry>0:
                if await self._is_terminal_closed(symbol):
                    try:
                        await self.storage.remove_position(symbol)
                    except Exception:
                        pass
                    continue
                pos.setdefault("initial_stop_price", stop)
                pos.setdefault("initial_take_price", take)
                pos["stop_price"] = new_stop
                pos["breakeven_moved"] = True
                pos["updated_at"] = now
                await self.storage.upsert_position(pos)
                events.append({"type":"breakeven","symbol":symbol, "stop_price": new_stop})
                pos["protection_checked_at"] = 0
                wd = await self._protection_watchdog(pos, live)
                if wd:
                    events.append(wd)
                stop = new_stop
            if is_liquidity_retest:
                pos.setdefault("initial_stop_price", stop)
                pos.setdefault("initial_take_price", take)
                runner_move, runner_stop, runner_stage = await self._liquidity_runner_stop(pos, price, stop)
                if runner_move:
                    if await self._is_terminal_closed(symbol):
                        try:
                            await self.storage.remove_position(symbol)
                        except Exception:
                            pass
                        continue
                    pos["stop_price"] = runner_stop
                    pos["liquidity_runner_stage"] = runner_stage
                    pos["liquidity_runner_enabled"] = True
                    pos["updated_at"] = now
                    await self.storage.upsert_position(pos)
                    events.append({"type":"liquidity_runner", "symbol": symbol, "stop_price": runner_stop, "stage_r": runner_stage})
                    pos["protection_checked_at"] = 0
                    wd = await self._protection_watchdog(pos, live)
                    if wd:
                        events.append(wd)
                    stop = runner_stop
            boost_monitor_only_no_exchange = False
            if is_boost_scalping and live:
                mode = str(pos.get("protection_mode") or "").lower()
                status_txt = str(pos.get("protection_status") or "").upper()
                monitor_only = str(await self._setting("boost_no_exchange_protection_monitor_only", os.getenv("BOOST_NO_EXCHANGE_PROTECTION_MONITOR_ONLY", "true"))).lower() in {"1", "true", "yes", "on"}
                boost_monitor_only_no_exchange = monitor_only and not (mode in {"exchange", "exchange_emergency_sl_only"} or status_txt in {"EXCHANGE PROTECTED", "TP + LIQUIDATION STOP", "EMERGENCY SL ONLY"})

                # v0219: HUNTER fast-profit extraction. BOOST/HUNTER should not
                # sit in a positive impulse for too long waiting for a large
                # trailing exit. Close only after MEXC confirms real positive
                # exchange PnL, so this still avoids the old fake-paper-profit
                # minus closes.
                try:
                    fast_enabled = str(await self._setting("boost_fast_profit_enabled", True)).lower() in {"1", "true", "yes", "on"}
                    if fast_enabled:
                        age_sec = max(0.0, now - opened)
                        min_age = float(await self._setting("boost_fast_profit_min_age_sec", 3) or 3)
                        min_pct = float(await self._setting("boost_fast_profit_min_pct", 0.11) or 0.11)
                        ex_min = float(await self._setting("boost_fast_profit_exchange_min_pct", 0.09) or 0.09)
                        max_hold = float(await self._setting("boost_fast_profit_max_hold_sec", 24) or 24)
                        trail_start = float(await self._setting("boost_fast_trailing_start_pct", 0.030) or 0.030)
                        trail_giveback = float(await self._setting("boost_fast_trailing_giveback_pct", 0.010) or 0.010)
                        decay_profit = float(await self._setting("boost_momentum_decay_profit_pct", 0.010) or 0.010)
                        best = float(pos.get("best_pnl_pct") or pnl)
                        fast_reason = ""
                        if age_sec >= min_age and pnl >= min_pct:
                            fast_reason = "boost_fast_profit"
                        elif age_sec >= min_age and best >= trail_start and pnl > 0 and (best - pnl) >= trail_giveback:
                            fast_reason = "boost_fast_trailing_lock"
                        elif age_sec >= max_hold and pnl >= decay_profit:
                            fast_reason = "boost_max_hold_profit_lock"
                        if fast_reason:
                            ok_profit, why_profit = await self._live_boost_profit_confirmed(pos, pnl, ex_min)
                            if ok_profit:
                                ev = await self._close_and_event(pos, "tp", fast_reason, live, price)
                                if ev:
                                    events.append(ev)
                                continue
                            else:
                                pos["boost_fast_profit_skip_reason"] = why_profit
                                # v0222 REAL silent wait mode:
                                # Tell Telegram only once per opened position that local
                                # fast-profit is waiting for real exchange profit. After
                                # that, keep updating storage silently until one of the
                                # real state changes happens: profit confirmed -> close,
                                # momentum decay/trailing exit, rotation, unsafe, or exit.
                                wait_key = f"{symbol}:{pos.get('opened_at')}:boost_fast_profit_wait_exchange_profit"
                                last_wait_key = str(pos.get("boost_fast_profit_wait_event_key") or "")
                                emit_wait = last_wait_key != wait_key
                                pos["boost_fast_profit_wait_event_key"] = wait_key
                                pos["boost_fast_profit_wait_reason"] = str(why_profit)
                                pos["boost_fast_profit_wait_silent"] = True
                                if emit_wait:
                                    pos["boost_fast_profit_wait_event_ts"] = now
                                pos["updated_at"] = now
                                await self.storage.upsert_position(pos)
                                if emit_wait:
                                    events.append({"type":"boost_fast_profit_wait_exchange_profit", "symbol": symbol, "reason": why_profit, "local_pnl_pct": pnl, "best_pnl_pct": best})
                except Exception as e:
                    events.append({"type":"boost_fast_profit_error", "symbol": symbol, "error": str(e)[:160]})

            if side=="LONG":
                if take and price>=take:
                    if live and strategy == "boost_scalping":
                        min_profit = float(await self._setting("boost_live_min_exchange_profit_pct", 0.09) or 0.09)
                        ok_profit, why_profit = await self._live_boost_profit_confirmed(pos, pnl, min_profit)
                        if not ok_profit:
                            pos["boost_tp_skip_reason"] = why_profit
                            last_reason = str(pos.get("boost_tp_wait_reason") or "")
                            last_ts = float(pos.get("boost_tp_wait_event_ts") or 0)
                            cooldown = float(await self._setting("boost_tp_wait_event_cooldown_sec", 30) or 30)
                            emit_wait = (str(why_profit) != last_reason) or (now - last_ts >= cooldown)
                            pos["boost_tp_wait_reason"] = str(why_profit)
                            if emit_wait:
                                pos["boost_tp_wait_event_ts"] = now
                            await self.storage.upsert_position(pos)
                            if emit_wait:
                                events.append({"type":"boost_tp_wait_exchange_profit", "symbol": symbol, "reason": why_profit})
                            continue
                    ev = await self._close_and_event(pos, "tp", "take_profit", live, price)
                    if ev: events.append(ev)
                    continue
                if stop and price<=stop and not liquidation_stop_mode:
                    if boost_monitor_only_no_exchange:
                        pos["boost_monitor_only_skip_sl"] = f"exchange TP/SL missing; local SL close skipped at pnl {pnl:+.3f}%"
                        pos["updated_at"] = now
                        await self.storage.upsert_position(pos)
                        events.append({"type":"boost_monitor_only_no_exchange_protection", "symbol": symbol, "pnl_pct": pnl})
                    else:
                        ev = await self._close_and_event(pos, "sl", "stop_loss", live, price)
                        if ev: events.append(ev)
                        continue
            else:
                if take and price<=take:
                    if live and strategy == "boost_scalping":
                        min_profit = float(await self._setting("boost_live_min_exchange_profit_pct", 0.09) or 0.09)
                        ok_profit, why_profit = await self._live_boost_profit_confirmed(pos, pnl, min_profit)
                        if not ok_profit:
                            pos["boost_tp_skip_reason"] = why_profit
                            last_reason = str(pos.get("boost_tp_wait_reason") or "")
                            last_ts = float(pos.get("boost_tp_wait_event_ts") or 0)
                            cooldown = float(await self._setting("boost_tp_wait_event_cooldown_sec", 30) or 30)
                            emit_wait = (str(why_profit) != last_reason) or (now - last_ts >= cooldown)
                            pos["boost_tp_wait_reason"] = str(why_profit)
                            if emit_wait:
                                pos["boost_tp_wait_event_ts"] = now
                            await self.storage.upsert_position(pos)
                            if emit_wait:
                                events.append({"type":"boost_tp_wait_exchange_profit", "symbol": symbol, "reason": why_profit})
                            continue
                    ev = await self._close_and_event(pos, "tp", "take_profit", live, price)
                    if ev: events.append(ev)
                    continue
                if stop and price>=stop and not liquidation_stop_mode:
                    if boost_monitor_only_no_exchange:
                        pos["boost_monitor_only_skip_sl"] = f"exchange TP/SL missing; local SL close skipped at pnl {pnl:+.3f}%"
                        pos["updated_at"] = now
                        await self.storage.upsert_position(pos)
                        events.append({"type":"boost_monitor_only_no_exchange_protection", "symbol": symbol, "pnl_pct": pnl})
                    else:
                        ev = await self._close_and_event(pos, "sl", "stop_loss", live, price)
                        if ev: events.append(ev)
                        continue
            if strategy == "quick_bounce":
                qb_time_stop = int(await self._setting("quick_bounce_time_stop_sec", os.getenv("QUICK_BOUNCE_TIME_STOP_SEC", "43200")) or 43200)
                if qb_time_stop > 0 and now - opened >= qb_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "quick_bounce_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if strategy == "impulse_dump":
                dump_time_stop = int(await self._setting("impulse_dump_time_stop_sec", os.getenv("IMPULSE_DUMP_TIME_STOP_SEC", "86400")) or 86400)
                if dump_time_stop > 0 and now - opened >= dump_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "impulse_dump_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if strategy == "orderflow_impulse":
                of_time_stop = int(await self._setting("orderflow_impulse_time_stop_sec", os.getenv("ORDERFLOW_IMPULSE_TIME_STOP_SEC", "86400")) or 86400)
                if of_time_stop > 0 and now - opened >= of_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "orderflow_impulse_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if strategy == "cascade_hunter":
                ch_time_stop = int(await self._setting("cascade_hunter_time_stop_sec", os.getenv("CASCADE_HUNTER_TIME_STOP_SEC", "14400")) or 14400)
                if ch_time_stop > 0 and now - opened >= ch_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "cascade_hunter_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if strategy == "strongest_coin":
                sc_time_stop = int(await self._setting("strongest_coin_time_stop_sec", os.getenv("STRONGEST_COIN_TIME_STOP_SEC", "600")) or 600)
                if sc_time_stop > 0 and now - opened >= sc_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "strongest_coin_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if not is_liquidity_retest:
                if manage_only_tpsl:
                    # v0181: For AI scalping and BOOST, do not choke a live trade with
                    # generic breakeven/trailing/time-stop. Local manager closes only
                    # on actual TP/SL; BOOST rotation/micro-TP is handled by BOOST engine.
                    pass
                else:
                    trailing_reason = policy.trailing_exit_reason(pos, pnl)
                    if trailing_reason:
                        ev = await self._close_and_event(pos, "trailing_exit", trailing_reason, live, price)
                        if ev: events.append(ev)
                        continue
                    time_reason = policy.time_stop_reason(pos, pnl, now-opened, time_stop_sec)
                    if time_reason:
                        ev = await self._close_and_event(pos, "time_stop", time_reason, live, price)
                        if ev: events.append(ev)
                        continue
            else:
                # v0082: liquidity_retest is not ultra-scalp. Keep only a long
                # hard safety timeout so dead retests don't live forever.
                lr_time_stop = int(await self._setting("liquidity_retest_time_stop_sec", os.getenv("LIQUIDITY_RETEST_TIME_STOP_SEC", "1800")) or 1800)
                if lr_time_stop > 0 and now - opened >= lr_time_stop:
                    ev = await self._close_and_event(pos, "time_stop", "liquidity_retest_time_stop", live, price)
                    if ev: events.append(ev)
                    continue
            if pos.get("best_pnl_pct") is not None:
                pos["updated_at"] = now
                await self.storage.upsert_position(pos)
        return events
