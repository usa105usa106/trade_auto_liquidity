import time
import os
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
            if state.get("protection_status") != "EXCHANGE PROTECTED":
                pos["protection_mode"] = "local_monitoring"
                pos["protection_warning"] = "exchange TP/SL not confirmed; bot monitors TP/SL locally"
            else:
                pos["protection_mode"] = "exchange"
                pos.pop("protection_warning", None)
            await self.storage.upsert_position(pos)
            if state.get("reattach_attempted") or state.get("protection_status") != "EXCHANGE PROTECTED":
                return {"type": "protection_watchdog", "symbol": pos.get("symbol"), **state}
        except Exception as e:
            pos["protection_warning"] = f"protection watchdog error: {str(e)[:180]}"
            pos["protection_checked_at"] = now
            await self.storage.upsert_position(pos)
            return {"type": "protection_watchdog_error", "symbol": pos.get("symbol"), "error": str(e)}
        return None

    async def _auto_close_on_protection_failed(self) -> bool:
        value = await self._setting("auto_close_on_protection_failed", os.getenv("ALLOW_AUTO_CLOSE_ON_PROTECTION_FAILED", "false"))
        return self._truthy(value, False)

    def pnl_pct(self, pos, price):
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return 0.0
        return (price-entry)/entry*100 if str(pos.get("side")).upper()=="LONG" else (entry-price)/entry*100

    async def _manage_pending(self, pos: dict, live: bool) -> dict | None:
        now = time.time()
        opened = float(pos.get("opened_at") or now)
        symbol = pos["symbol"]
        if not live:
            # Paper limit orders are simulated as filled on the next management
            # tick; no private exchange endpoint is needed, but the pending
            # lifecycle is still visible in storage after placement.
            pos["status"] = "open"
            pos["updated_at"] = now
            pos = self.execution_engine._decorate_position_metrics(pos)
            await self.storage.upsert_position(pos)
            return {"type": "paper_limit_filled", "symbol": symbol}

        # Timeout: cancel stale limit entry and free slot.
        _time_stop_sec, limit_timeout_sec, _breakeven_trigger_pct = await self._runtime_limits()
        if now - opened >= limit_timeout_sec:
            res = await self.execution_engine.cancel_entry(pos, live=True, reason="limit_timeout")
            return {"type": "limit_timeout", "symbol": symbol, "result": res}

        # Confirm the exact order state. Disappearance from open orders is not
        # enough: it may mean filled, canceled, rejected, or expired.
        oid = str(pos.get("order_id") or "")
        if not oid:
            return {"type": "pending_sync_warning", "symbol": symbol, "error": "missing order_id"}
        try:
            order = await self.execution_engine.exchange_client.fetch_order(oid, symbol)
            status = str(order.get("status") or "").lower()
            filled = float(order.get("filled") or 0)
            amount = float(order.get("amount") or pos.get("qty") or 0)
            avg = float(order.get("average") or order.get("price") or pos.get("entry_price") or 0)
            if status in {"closed", "filled"} or (amount > 0 and filled >= amount * 0.999):
                pos["status"] = "open"
                pos["entry_price"] = avg or pos.get("entry_price")
                pos["updated_at"] = now
                pos = self.execution_engine._decorate_position_metrics(pos)
                protection = await self.execution_engine.place_protection_orders(pos, live=True)
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
                    if await self._auto_close_on_protection_failed():
                        close_res = await self.execution_engine.close_position(pos, "protection_failed", live=True, exit_price=pos.get("entry_price"))
                        return {"type": "protection_failed", "symbol": symbol, "result": close_res, "protection": protection}
                    return {"type": "protection_local", "symbol": symbol, "protection": protection}
                return {"type": "limit_filled", "symbol": symbol}
            if status in {"canceled", "cancelled", "rejected", "expired"}:
                await self.storage.remove_position(symbol)
                await self.storage.set_lock(symbol, 30, f"limit_{status}")
                return {"type": f"limit_{status}", "symbol": symbol}
        except Exception as e:
            # On sync error keep pending but don't duplicate entries; occupied slot remains used.
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
            try:
                price=await price_provider(symbol)
            except Exception as e:
                events.append({"type":"price_error","symbol":symbol,"error":str(e)})
                continue
            if not price:
                continue
            side=str(pos.get("side")).upper(); stop=float(pos.get("stop_price") or 0); take=float(pos.get("take_price") or 0); entry=float(pos.get("entry_price") or 0); opened=float(pos.get("opened_at") or now); pnl=self.pnl_pct(pos, price)
            strategy = str(pos.get("strategy") or "").lower()
            is_liquidity_retest = strategy == "liquidity_retest"
            policy = await self._refresh_scalp_policy()
            policy.update_best_pnl(pos, pnl)
            if is_liquidity_retest:
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
            if side=="LONG":
                if take and price>=take:
                    ev = await self._close_and_event(pos, "tp", "take_profit", live, price)
                    if ev: events.append(ev)
                    continue
                if stop and price<=stop:
                    ev = await self._close_and_event(pos, "sl", "stop_loss", live, price)
                    if ev: events.append(ev)
                    continue
            else:
                if take and price<=take:
                    ev = await self._close_and_event(pos, "tp", "take_profit", live, price)
                    if ev: events.append(ev)
                    continue
                if stop and price>=stop:
                    ev = await self._close_and_event(pos, "sl", "stop_loss", live, price)
                    if ev: events.append(ev)
                    continue
            if not is_liquidity_retest:
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
