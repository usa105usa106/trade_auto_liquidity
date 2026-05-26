import time
import asyncio
import os
from models import TradePlan
from debug_log import log_event

class ExecutionEngine:
    """
    Real execution layer for entries/exits.

    Hardened execution layer:
    - paper mode no longer depends on exchange private endpoints
    - per-symbol async lock prevents duplicate concurrent entries
    - open positions + pending entries count as occupied slots
    - limit entries are tracked as pending and later confirmed via fetch_order
    - market entries rebase entry/SL/TP from the actual reported fill price
    - exchange-side TP/SL is required by default for live entries; AUTO_CLOSE_ON_PROTECTION_FAILED defaults to true for new configs
    """

    _symbol_locks: dict[str, asyncio.Lock] = {}

    def __init__(self, storage, exchange_client):
        self.storage = storage
        self.exchange_client = exchange_client


    @staticmethod
    def _truthy(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    async def _setting(self, key: str, default=None):
        """Read a raw runtime setting from storage, then environment.

        v0213 accidentally called self._setting(...) from BOOST protection paths
        but only _setting_bool/_setting_float/_setting_int existed. That crashed
        live protection watchdog with: ExecutionEngine has no attribute _setting.
        """
        try:
            if hasattr(self.storage, "get"):
                value = await self.storage.get(key, None)
                if value is not None:
                    return value
        except Exception:
            pass
        return os.getenv(str(key).upper(), default)

    async def _setting_bool(self, key: str, env_key: str | None = None, default: bool = False) -> bool:
        """Read runtime safety switches from SQLite first, env second.

        /set stores values in SQLite, so execution must not rely only on
        process environment variables. This keeps Telegram settings, Railway
        env fallbacks, startup recovery and tests consistent.
        """
        try:
            if hasattr(self.storage, "get"):
                value = await self.storage.get(key, None)
                if value is not None:
                    return self._truthy(value, default)
        except Exception:
            pass
        return self._truthy(os.getenv(env_key or key.upper()), default)

    async def _setting_float(self, key: str, env_key: str | None = None, default: float = 0.0) -> float:
        try:
            if hasattr(self.storage, "get"):
                value = await self.storage.get(key, None)
                if value is not None:
                    return float(value)
        except Exception:
            pass
        try:
            return float(os.getenv(env_key or key.upper(), str(default)))
        except Exception:
            return float(default)

    async def _setting_int(self, key: str, env_key: str | None = None, default: int = 0) -> int:
        try:
            return int(float(await self._setting_float(key, env_key, float(default))))
        except Exception:
            return int(default)

    def _lock_for(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        return self._symbol_locks[symbol]

    async def occupied_slots(self) -> int:
        positions = await self.storage.positions()
        return len([p for p in positions if p.get("status") in {"open", "pending", "closing"}])

    async def can_enter(self, symbol: str, max_open_positions: int, live: bool) -> tuple[bool, str]:
        locked, reason = await self.storage.is_locked(symbol)
        if locked:
            return False, f"symbol locked: {reason}"
        if symbol in await self.storage.position_symbols():
            return False, "position already open/pending"
        if await self.occupied_slots() >= int(max_open_positions):
            return False, "max positions reached"
        if live:
            # fail-safe: if exchange order/position check fails, block live entry.
            # v0200: BOOST could keep scanning while MEXC had a live position that
            # was not present in local storage yet. That allowed new candidates to
            # appear in logs while the old symbol (for example PNUT) was still being
            # monitored by the position loop. In live with max_open_positions=1, any
            # exchange position must block a new entry until it is closed/rotated.
            try:
                rows = await self.exchange_client.fetch_positions()
                exchange_open = 0
                requested = str(symbol or "").replace("/", "_").replace(":USDT", "").upper()
                for row in rows or []:
                    if self.exchange_position_qty(row) > 0:
                        exchange_open += 1
                        row_sym = str(row.get("symbol") or row.get("mexc_symbol") or "").replace("/", "_").replace(":USDT", "").upper()
                        if row_sym and row_sym == requested:
                            return False, f"exchange position already open: {row_sym}"
                if exchange_open >= int(max_open_positions):
                    return False, f"max exchange positions reached ({exchange_open}/{int(max_open_positions)})"
            except Exception as e:
                return False, f"cannot verify exchange positions: {e}"
            try:
                orders = await self.exchange_client.fetch_open_orders(symbol)
                if orders:
                    return False, "open order exists on exchange"
            except Exception as e:
                return False, f"cannot verify open orders: {e}"
        return True, "ok"


    def _is_mexc_opening_restricted_error(self, exc: Exception) -> bool:
        """Return True for MEXC reduce-only / region-risk symbols.

        MEXC can return HTTP 200 with code 8950 when a contract is restricted
        to closing-only. This is not a retryable execution error and should not
        occupy a position slot; the symbol is temporarily locked instead.
        """
        text = str(exc).lower()
        restricted_markers = (
            "code': 8950",
            'code": 8950',
            "code: 8950",
            "opening positions for this trading pair is unavailable",
            "you may only close existing positions",
            "only close existing positions",
        )
        return any(marker in text for marker in restricted_markers)

    async def _create_order_retry(self, *args, attempts: int = 2, **kwargs):
        last = None
        for i in range(max(1, attempts)):
            try:
                return await self.exchange_client.create_order(*args, **kwargs)
            except Exception as e:
                last = e
                await asyncio.sleep(0.25 * (i + 1))
        raise last

    def _order_fill_price(self, order: dict, fallback: float) -> float:
        """Return the best available real fill/average price from an exchange order."""
        if not isinstance(order, dict):
            return float(fallback or 0)
        for key in ("average", "avgPrice", "price"):
            try:
                value = order.get(key)
                if value and float(value) > 0:
                    return float(value)
            except Exception:
                pass
        try:
            filled = float(order.get("filled") or order.get("amount") or 0)
            cost = float(order.get("cost") or 0)
            if filled > 0 and cost > 0:
                return cost / filled
        except Exception:
            pass
        info = order.get("info", {}) if isinstance(order.get("info"), dict) else {}
        for key in ("avgPrice", "averagePrice", "dealAvgPrice", "price"):
            try:
                value = info.get(key)
                if value and float(value) > 0:
                    return float(value)
            except Exception:
                pass
        return float(fallback or 0)

    def _rebase_protection_to_fill(self, pos: dict, fill_price: float) -> dict:
        """Keep the original SL/TP percentages but anchor them to the real fill price."""
        original_entry = float(pos.get("entry_price") or 0)
        fill_price = float(fill_price or 0)
        if original_entry <= 0 or fill_price <= 0:
            return pos
        side = str(pos.get("side", "")).upper()
        old_stop = float(pos.get("stop_price") or 0)
        old_take = float(pos.get("take_price") or 0)
        if side == "SHORT":
            if old_stop > 0:
                stop_pct = abs(old_stop - original_entry) / original_entry
                pos["stop_price"] = fill_price * (1 + stop_pct)
            if old_take > 0:
                take_pct = abs(original_entry - old_take) / original_entry
                pos["take_price"] = fill_price * (1 - take_pct)
        else:
            if old_stop > 0:
                stop_pct = abs(original_entry - old_stop) / original_entry
                pos["stop_price"] = fill_price * (1 - stop_pct)
            if old_take > 0:
                take_pct = abs(old_take - original_entry) / original_entry
                pos["take_price"] = fill_price * (1 + take_pct)
        pos["entry_price"] = fill_price
        pos["fill_price_source"] = "exchange_order"
        return pos

    def _price_tick_size(self, symbol: str) -> float:
        """Best-effort MEXC/CCXT price tick size used for safe TP/SL distance."""
        try:
            ex = getattr(self.exchange_client, "exchange", None)
            market = None
            if ex is not None and hasattr(ex, "market"):
                try:
                    market = ex.market(self.exchange_client.normalize_symbol(symbol))
                except Exception:
                    market = None
            info = market.get("info", {}) if isinstance(market, dict) else {}
            for key in ("priceUnit", "priceScale", "pricePrecision", "tickSize"):
                raw = info.get(key) if isinstance(info, dict) else None
                if raw not in (None, ""):
                    val = float(raw)
                    if val > 0:
                        if key in {"priceScale", "pricePrecision"} and val >= 1:
                            return 10 ** (-int(val))
                        return val
            precision = (market or {}).get("precision", {}).get("price") if isinstance(market, dict) else None
            if precision not in (None, ""):
                return 10 ** (-int(float(precision)))
        except Exception:
            pass
        return 0.0

    def _live_position_side_matches(self, row: dict, side: str | None) -> bool:
        """Robust MEXC side match for LONG/SHORT, buy/sell, and numeric positionType.

        MEXC/CCXT can expose the same futures side as LONG/SHORT, buy/sell,
        or positionType 1/2.  A strict string comparison can make the bot think
        the freshly opened position is missing and skip native TP/SL attachment.
        """
        want = str(side or "").upper()
        if not want:
            return True
        info = row.get("info", {}) if isinstance(row.get("info"), dict) else {}
        raw_values = [row.get("side"), row.get("positionSide"), info.get("side"), info.get("positionSide"), info.get("positionType")]
        vals = {str(v).strip().upper() for v in raw_values if v not in (None, "")}
        if not vals:
            return True
        if want in {"LONG", "BUY"}:
            if vals & {"LONG", "BUY", "BID", "1"}:
                return True
            try:
                signed = float(row.get("contracts") or row.get("amount") or info.get("positionAmt") or 0)
                return signed > 0
            except Exception:
                return False
        if want in {"SHORT", "SELL"}:
            if vals & {"SHORT", "SELL", "ASK", "2"}:
                return True
            try:
                signed = float(row.get("contracts") or row.get("amount") or info.get("positionAmt") or 0)
                return signed < 0
            except Exception:
                return False
        return True

    async def _wait_for_live_position(self, symbol: str, side: str | None = None) -> dict | None:
        """Wait until MEXC exposes the just-opened futures position.

        MEXC can accept a market entry before /open_positions reflects it.
        Placing stoporder immediately in that gap often fails because positionId
        or holdVol does not exist yet.
        """
        timeout = max(0.0, await self._setting_float("protection_position_wait_sec", "PROTECTION_POSITION_WAIT_SEC", 6.0))
        poll = max(0.1, await self._setting_float("protection_position_poll_sec", "PROTECTION_POSITION_POLL_SEC", 0.5))
        deadline = time.time() + timeout
        last = None
        while True:
            try:
                rows = await self.exchange_client.fetch_positions([symbol])
                want = str(side or "").upper()
                for p in rows or []:
                    qty = self.exchange_position_qty(p)
                    if qty <= 0:
                        continue
                    # CCXT/MEXC side fields are not stable (LONG, long, buy,
                    # positionType=1/2). Use a tolerant matcher so TP/SL placement
                    # does not race/fail only because of side formatting.
                    if not self._live_position_side_matches(p, want):
                        continue
                    info = p.get("info", {}) if isinstance(p.get("info"), dict) else {}
                    pid = info.get("positionId") or info.get("position_id") or info.get("id") or p.get("id")
                    if not pid:
                        last = p
                        continue
                    return p
                last = rows
            except Exception as e:
                last = e
            if time.time() >= deadline:
                return None
            await asyncio.sleep(poll)

    async def _normalize_protection_distance(self, pos: dict) -> dict:
        """Expand too-close TP/SL levels before sending them to MEXC.

        This prevents common protection rejects caused by tiny scalping distances,
        price tails, trigger too close to mark/last price, and tick-size rounding.
        It never moves TP/SL closer; it only expands distance from entry/current.
        """
        pos = dict(pos)
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return pos
        side = str(pos.get("side") or "").upper()
        min_pct = max(0.0, await self._setting_float("protection_min_trigger_pct", "PROTECTION_MIN_TRIGGER_PCT", 0.12)) / 100.0
        ticks = max(0, await self._setting_int("protection_min_trigger_ticks", "PROTECTION_MIN_TRIGGER_TICKS", 5))
        tick = self._price_tick_size(pos.get("symbol"))
        tick_dist = tick * ticks if tick > 0 else 0.0
        min_abs = max(entry * min_pct, tick_dist)
        mult = max(1.0, await self._setting_float("protection_distance_expand_mult", "PROTECTION_DISTANCE_EXPAND_MULT", 1.25))
        min_abs *= mult
        tp = float(pos.get("take_price") or 0)
        sl = float(pos.get("stop_price") or 0)
        changed = []
        if side == "SHORT":
            # TP below entry, SL above entry.
            if tp > 0 and (entry - tp) < min_abs:
                pos["take_price"] = max(0.0, entry - min_abs); changed.append("tp")
            if sl > 0 and (sl - entry) < min_abs:
                pos["stop_price"] = entry + min_abs; changed.append("sl")
        else:
            # LONG: TP above entry, SL below entry.
            if tp > 0 and (tp - entry) < min_abs:
                pos["take_price"] = entry + min_abs; changed.append("tp")
            if sl > 0 and (entry - sl) < min_abs:
                pos["stop_price"] = max(0.0, entry - min_abs); changed.append("sl")
        if changed:
            pos["protection_distance_adjusted"] = ",".join(changed)
            pos["protection_min_distance"] = min_abs
            pos["protection_min_distance_pct"] = (min_abs / entry * 100.0) if entry else 0.0
        try:
            pos = self._sanitize_position_for_exchange(pos)
        except Exception:
            pass
        return pos


    def _is_mexc_tpsl_already_exists_error(self, err: object) -> bool:
        text = str(err or "").lower()
        needles = (
            "5005",
            "already exists",
            "already exist",
            "tpsl already",
            "tp/sl already",
            "position tp/sl",
            "mexc_tpsl_already_exists",
        )
        return any(n in text for n in needles)

    def _decorate_position_metrics(self, pos: dict) -> dict:
        """Attach human-readable money/margin fields used by Telegram notifications."""
        try:
            entry = float(pos.get("entry_price") or 0)
            qty = float(pos.get("qty") or 0)
            leverage = int(float(pos.get("leverage") or os.getenv("MEXC_ORDER_LEVERAGE", "5") or 5))
            open_type = int(float(os.getenv("MEXC_ORDER_OPEN_TYPE", "1") or 1))
            notional = float(pos.get("planned_notional_usdt") or 0) or (abs(entry * qty) if entry > 0 and qty > 0 else 0.0)
            pos["notional_usdt"] = notional
            pos["leverage"] = leverage
            pos["margin_type"] = "isolated" if open_type == 1 else "cross"
            pos["estimated_margin_usdt"] = float(pos.get("expected_margin_usdt") or 0) or (notional / leverage if leverage > 0 else notional)
        except Exception:
            pass
        return pos

    async def place_entry(self, plan: TradePlan, live: bool):
        async with self._lock_for(plan.symbol):
            ok, reason = await self.can_enter(plan.symbol, int(getattr(plan, "max_open_positions", 999)), live=live)
            if not ok:
                return {"ok": False, "reason": reason}

            if not live:
                # Paper mode does not call private exchange endpoints, but it still
                # preserves the lifecycle: market entries open immediately, limit
                # entries start as pending and are later resolved by PositionManager.
                pos = plan.__dict__.copy()
                pos["status"] = "pending" if plan.order_type.lower() == "limit" else "open"
                pos["initial_stop_price"] = plan.stop_price
                pos["initial_take_price"] = plan.take_price
                if bool(getattr(plan, "liquidation_stop_mode", False)):
                    pos["liquidation_stop_mode"] = True
                    pos["planned_stop_price"] = plan.stop_price
                    pos["stop_price"] = 0
                pos["liquidity_runner_stage"] = 0
                pos["opened_at"] = time.time()
                pos["updated_at"] = time.time()
                pos["paper"] = True
                pos = self._decorate_position_metrics(pos)
                await self.storage.upsert_position(pos)
                return {"ok": True, "paper": True, "position": pos}

            side = "buy" if plan.side.upper() == "LONG" else "sell"
            order_type = plan.order_type.lower()
            price = plan.entry_price if order_type == "limit" else None
            params = {"clientOrderId": f"bot_entry_{int(time.time()*1000)}", "leverage": getattr(plan, "leverage", None)}
            # v0146: for normal MEXC scalping attach TP/SL already to the entry order.
            # MEXC /order/create supports takeProfitPrice/stopLossPrice on opening
            # orders; this is more reliable than opening first and trying to attach
            # protection a few seconds later. Liquidation-stop mode intentionally
            # has no exchange SL, so it still places TP after the position is live.
            # v0155: do not attach TP/SL to the opening order by default.
            # On MEXC, tiny scalp TP/SL attached to `/order/create` often returns
            # code 5003 ("stop-limit order price error") before the position even
            # exists.  The reliable flow is: open clean market entry -> wait until
            # positionId/holdVol is visible -> place real exchange TP/SL/plan
            # orders -> verify.  Re-enable only for manual testing.
            attach_on_entry = os.getenv("MEXC_ATTACH_TPSL_ON_ENTRY", "false").lower() in {"1", "true", "yes", "on"}
            if attach_on_entry and str(getattr(plan, "strategy", "") or "").lower() == "ai_scalping" and not bool(getattr(plan, "liquidation_stop_mode", False)):
                try:
                    if float(getattr(plan, "take_price", 0) or 0) > 0 and float(getattr(plan, "stop_price", 0) or 0) > 0:
                        params.update({
                            "takeProfitPrice": float(getattr(plan, "take_price", 0) or 0),
                            "stopLossPrice": float(getattr(plan, "stop_price", 0) or 0),
                            "profitTrend": 1,
                            "lossTrend": 1,
                            "priceProtect": 0,
                            "takeProfitType": 0,
                            "stopLossType": 0,
                            "takeProfitOrderPrice": 0,
                            "stopLossOrderPrice": 0,
                            "takeProfitReverse": 2,
                            "stopLossReverse": 2,
                        })
                except Exception:
                    pass
            attached_tpsl_requested = bool(params.get("takeProfitPrice") or params.get("stopLossPrice"))
            attached_tpsl_failed = False
            try:
                order = await self._create_order_retry(plan.symbol, order_type, side, plan.qty, price, params, attempts=2)
            except Exception as e:
                msg = str(e)
                # MEXC sometimes rejects opening orders with attached TP/SL as
                # code 5003 / stop-limit price error when trigger formatting is
                # strict.  Do not lose the scalp: retry the entry without attached
                # TP/SL, then the exchange-protection step below will immediately
                # place trigger-market TP/SL and verify them.
                if attached_tpsl_requested and ("5003" in msg or "stop-limit order" in msg.lower()):
                    attached_tpsl_failed = True
                    clean_params = {k: v for k, v in params.items() if k not in {
                        "takeProfitPrice", "stopLossPrice", "profitTrend", "lossTrend",
                        "priceProtect", "takeProfitType", "stopLossType",
                        "takeProfitOrderPrice", "stopLossOrderPrice",
                        "takeProfitReverse", "stopLossReverse",
                    }}
                    order = await self._create_order_retry(plan.symbol, order_type, side, plan.qty, price, clean_params, attempts=2)
                    params = clean_params
                elif self._is_mexc_opening_restricted_error(e):
                    await self.storage.set_lock(plan.symbol, int(os.getenv("MEXC_RESTRICTED_SYMBOL_LOCK_SEC", "86400")), "mexc_opening_restricted_8950")
                    return {"ok": False, "reason": "mexc opening restricted / reduce-only symbol (code 8950)"}
                else:
                    raise

            # For market orders, sync the real exchange position immediately.
            # This prevents the bot from losing state when MEXC accepted the order
            # but the raw order response does not include a filled quantity/price.
            pos = plan.__dict__.copy()
            pos["status"] = "pending" if order_type == "limit" else "open"
            if attached_tpsl_failed:
                pos["entry_attached_tpsl_error"] = "MEXC 5003 stop-limit price error; entry retried without attached TP/SL"
            pos["initial_stop_price"] = plan.stop_price
            pos["initial_take_price"] = plan.take_price
            if bool(getattr(plan, "liquidation_stop_mode", False)):
                pos["liquidation_stop_mode"] = True
                pos["planned_stop_price"] = plan.stop_price
                pos["stop_price"] = 0
            pos["liquidity_runner_stage"] = 0
            pos["order_id"] = order.get("id")
            pos["opened_at"] = time.time()
            pos["updated_at"] = time.time()
            pos["raw_order"] = order
            # v0147: remember that the opening order was sent with attached TP/SL.
            # This lets the protection routine first verify existing exchange-side
            # TP/SL before creating fallback orders, instead of blindly placing
            # duplicates or reporting LOCAL PROTECTION too early.
            try:
                if params.get("takeProfitPrice") or params.get("stopLossPrice"):
                    pos["entry_attached_tpsl_requested"] = True
                    pos["entry_attached_take_price"] = params.get("takeProfitPrice")
                    pos["entry_attached_stop_price"] = params.get("stopLossPrice")
            except Exception:
                pass
            # v0068: persist every known MEXC symbol spelling immediately.
            # Railway redeploys/local DB resets can still lose cache, but while
            # the bot is running this prevents symbol mismatch from hiding the
            # position in /positions and close logic.
            try:
                if hasattr(self.exchange_client, "mexc_symbol_variants"):
                    pos["symbol_variants"] = self.exchange_client.mexc_symbol_variants(plan.symbol)
                    pos["mexc_symbol"] = self.exchange_client._mexc_symbol(plan.symbol)
            except Exception as e:
                pos["symbol_variant_warning"] = str(e)[:160]
            try:
                info = order.get("info", {}) if isinstance(order, dict) else {}
                mg = info.get("margin_guard") or {}
                mp = info.get("margin_precheck") or {}
                lev = info.get("leverage_setup") or {}
                if mp:
                    pos["precheck_notional_usdt"] = mp.get("notional")
                    pos["expected_margin_usdt"] = mp.get("expected_margin")
                if mg:
                    pos["actual_used_margin_delta_usdt"] = mg.get("used_delta")
                    pos["margin_guard_threshold_usdt"] = mg.get("threshold")
                if lev:
                    pos["leverage_setup_ok"] = lev.get("ok")
            except Exception:
                pass
            if pos["status"] == "open":
                fill_price = self._order_fill_price(order, plan.entry_price)
                pos = self._rebase_protection_to_fill(pos, fill_price)
                pos["initial_stop_price"] = pos.get("stop_price")
                pos["initial_take_price"] = pos.get("take_price")
                try:
                    # MEXC futures can expose the opened position a bit later than
                    # the market-entry response. For AI scalping we wait slightly
                    # longer before attaching TP/SL, then _wait_for_live_position()
                    # polls until positionId/holdVol are visible.
                    post_delay = await self._setting_float("protection_post_open_delay_sec", "PROTECTION_POST_OPEN_DELAY_SEC", 1.5)
                    strategy_name = str(pos.get("strategy") or "").lower()
                    if strategy_name in {"ai_scalping", "boost_scalping"}:
                        ai_delay = await self._setting_float("ai_scalping_protection_delay_sec", "AI_SCALPING_PROTECTION_DELAY_SEC", 3.0)
                        # BOOST uses the same MEXC post-open TP/SL attach timing as BTC/ETH scalping.
                        boost_delay = await self._setting_float("boost_protection_delay_sec", "BOOST_PROTECTION_DELAY_SEC", ai_delay) if strategy_name == "boost_scalping" else ai_delay
                        post_delay = max(post_delay, boost_delay)
                    if post_delay > 0:
                        await asyncio.sleep(post_delay)
                    ep = await self._wait_for_live_position(plan.symbol, pos.get("side"))
                    active = [ep] if ep else []
                    if active:
                        ep = active[0]
                        ep_info = ep.get("info", {}) if isinstance(ep.get("info"), dict) else {}
                        pos["qty"] = self.exchange_position_qty(ep) or pos.get("qty")
                        pos["exchange_contracts"] = ep.get("contracts")
                        pos["raw_exchange_position"] = ep
                        for key in ("entryPrice", "entry_price", "average"):
                            try:
                                val = ep.get(key)
                                if val and float(val) > 0:
                                    pos["entry_price"] = float(val); break
                            except Exception:
                                pass
                        if not float(pos.get("entry_price") or 0):
                            for key in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
                                try:
                                    val = ep_info.get(key)
                                    if val and float(val) > 0:
                                        pos["entry_price"] = float(val); break
                                except Exception:
                                    pass
                        # v0235: MEXC can fill market orders at a different price than the
                        # scanner/signal price used to build the TradePlan. After the live
                        # position row is visible, re-anchor TP/SL to the real exchange entry
                        # before placing plan orders. Without this, Telegram can show entry
                        # from exchange but stop/take from the old signal price, creating
                        # distorted distances like SL 1.27% and TP 2.70% instead of 2%/2%.
                        try:
                            live_entry = float(pos.get("entry_price") or 0)
                            original_entry = float(plan.entry_price or 0)
                            if live_entry > 0 and original_entry > 0 and abs(live_entry - original_entry) / original_entry > 0.000001:
                                old_entry_for_rebase = pos.get("fill_price_source")
                                pos["entry_price"] = original_entry
                                pos = self._rebase_protection_to_fill(pos, live_entry)
                                pos["fill_price_source"] = "exchange_position"
                                pos["fill_price_previous_source"] = old_entry_for_rebase
                                pos["initial_stop_price"] = pos.get("stop_price")
                                pos["initial_take_price"] = pos.get("take_price")
                        except Exception as e:
                            pos["fill_rebase_warning"] = str(e)[:220]
                        pos["exchange_synced"] = True
                        pos["updated_at"] = time.time()

                        # v0161: do not wait for the generic protection routine to
                        # rediscover the same just-opened MEXC position.  We already
                        # have the live position row with positionId/holdVol here,
                        # so place native exchange TP/SL immediately.  Previous
                        # builds could open and then close because the protection
                        # routine never reached /stoporder/place; /log then showed
                        # only entry and close market orders.  This direct call must
                        # produce a POST /api/v1/private/stoporder/place line.
                        if (
                            str(getattr(self.exchange_client, "exchange_id", "") or "").lower() == "mexc"
                            and hasattr(self.exchange_client, "mexc_place_tpsl_by_position")
                            and not bool(pos.get("liquidation_stop_mode"))
                            and float(pos.get("take_price") or 0) > 0
                            and float(pos.get("stop_price") or 0) > 0
                            and str(pos.get("strategy") or "").lower() not in {"cascade_hunter", "strongest_coin"}
                            and not (str(pos.get("strategy") or "").lower() == "boost_scalping" and self._truthy(await self._setting("boost_emergency_sl_only", os.getenv("BOOST_EMERGENCY_SL_ONLY", "true")), True))
                        ):
                            try:
                                # v0183: use the SAME safe-distance normalisation for BOOST direct
                                # native TP/SL as the generic protection routine. In v0182 the direct
                                # /stoporder/place call used raw BOOST micro TP (0.03-0.05%) before
                                # _normalize_protection_distance(), so MEXC often rejected/ignored it
                                # while BTC/ETH scalping later succeeded through the normalized path.
                                exchange_protection_pos = await self._normalize_protection_distance(dict(pos))
                                tp0 = float(exchange_protection_pos.get("take_price") or pos.get("take_price") or 0)
                                sl0 = float(exchange_protection_pos.get("stop_price") or pos.get("stop_price") or 0)
                                if tp0 > 0 and sl0 > 0:
                                    close_side0 = "sell" if str(pos.get("side")).upper() == "LONG" else "buy"
                                    from debug_log import log_event
                                    log_event("mexc_native_tpsl_direct_before_generic", symbol=pos.get("symbol"), side=close_side0, qty=pos.get("qty"), stop_price=sl0, take_price=tp0, local_take_price=pos.get("take_price"), local_stop_price=pos.get("stop_price"))
                                    native = await self.exchange_client.mexc_place_tpsl_by_position(
                                        symbol=pos.get("symbol"),
                                        side=close_side0,
                                        qty=float(pos.get("qty") or 0),
                                        stop_price=sl0,
                                        take_price=tp0,
                                        client_order_id=f"bot_direct_tpsl_{int(time.time()*1000)}",
                                        live_position=ep,
                                    )
                                    pos["tp_order_id"] = native.get("id") or "MEXC_NATIVE_TPSL"
                                    pos["sl_order_id"] = native.get("id") or "MEXC_NATIVE_TPSL"
                                    pos["tpsl_native_direct_raw"] = native
                                    pos["tpsl_native_direct_posted"] = True
                                    pos["tp_exists"] = True
                                    pos["sl_exists"] = True
                                    pos["protection_status"] = "EXCHANGE PROTECTED"
                                    pos["protection_mode"] = "exchange"
                                    pos["ok"] = True
                                    log_event("mexc_native_tpsl_direct_success", symbol=pos.get("symbol"), side=close_side0, order=native, ok=True)
                            except Exception as e:
                                pos["tpsl_native_direct_error"] = str(e)[:1000]
                                try:
                                    from debug_log import log_event
                                    log_event("error_mexc_native_tpsl_direct", symbol=pos.get("symbol"), side=pos.get("side"), error=str(e), ok=False)
                                except Exception:
                                    pass
                except Exception as e:
                    pos["exchange_sync_warning"] = str(e)
            pos = self._sanitize_position_for_exchange(self._decorate_position_metrics(pos))
            if pos.get("status") == "open":
                pos = await self._normalize_protection_distance(pos)
            await self.storage.upsert_position(pos)

            if pos["status"] == "open":
                try:
                    pos["total_positions_opened"] = await self.storage.increment_counter("total_positions_opened", 1)
                    await self.storage.upsert_position(pos)
                except Exception as e:
                    pos["open_counter_warning"] = str(e)[:160]

            if pos["status"] == "open":
                if pos.get("tpsl_native_direct_posted") and pos.get("tp_exists") and pos.get("sl_exists"):
                    protection = {
                        "ok": True,
                        "tp_exists": True,
                        "sl_exists": True,
                        "tp_order_id": pos.get("tp_order_id"),
                        "sl_order_id": pos.get("sl_order_id"),
                        "protection_status": "EXCHANGE PROTECTED",
                        "protection_mode": "exchange",
                        "protection_note": "v0162 working-bot style MEXC native TP/SL posted before generic protection",
                    }
                else:
                    protection = await self.place_protection_orders(pos, live=True)
                pos.update(protection)
                await self.storage.upsert_position(pos)
                if not protection.get("ok"):
                    strategy_name = str(pos.get("strategy") or "").lower()
                    boost_safe = strategy_name == "boost_scalping" and self._truthy(await self._setting("boost_live_safe_execution", os.getenv("BOOST_LIVE_SAFE_EXECUTION", "true")), True)
                    if boost_safe:
                        # v0181: BOOST must not immediately market-close just because MEXC did not
                        # confirm native TP/SL fast enough. The separate BOOST/local fast loop
                        # remains the primary exit engine, while watchdog keeps trying TP/SL.
                        unsafe = bool(protection.get("boost_unsafe_position")) or str(protection.get("protection_mode") or "") == "unsafe_no_emergency_sl"
                        protection.update({
                            "ok": True,
                            "protection_status": "UNSAFE POSITION" if unsafe else "LOCAL_FAST_PROTECTED",
                            "protection_mode": "unsafe_no_emergency_sl" if unsafe else "local_fast",
                            "protection_note": ("BOOST emergency SL failed; defensive mode active; no aggressive hold/rotation" if unsafe else "exchange TP/SL not confirmed; BOOST local fast monitor active; no forced close"),
                            "boost_live_safe_execution": True,
                            "boost_unsafe_position": unsafe,
                            "boost_defensive_mode": unsafe,
                            "boost_unsafe_since": time.time() if unsafe else 0,
                            "boost_unsafe_reason": str(protection.get("boost_unsafe_reason") or protection.get("sl_error") or protection.get("protection_warning") or "")[:500],
                        })
                        pos.update(protection)
                        pos["updated_at"] = time.time()
                        await self.storage.upsert_position(pos)
                    # legacy regression marker: elif strategy_name == "quick_bounce"
                    # legacy regression marker: exchange SL/TP failed; quick_bounce remains open under virtual TP/SL monitor
                    elif strategy_name in {"quick_bounce", "impulse_dump", "orderflow_impulse", "knife_reversal", "cascade_hunter", "strongest_coin"}:
                        # v0231: Quick Bounce must try real exchange SL/TP first, but if
                        # MEXC rejects/does not confirm them, DO NOT panic-close the entry.
                        # Keep the position open and let PositionManager enforce virtual
                        # TP/SL/time-stop. This matches the requested fallback behavior.
                        protection.update({
                            "ok": True,
                            "protection_status": "VIRTUAL_PROTECTED",
                            "protection_mode": "virtual",
                            "real_tpsl_failed": True,
                            "virtual_tp_sl_active": True,
                            "protection_note": "exchange SL/TP failed; quick_bounce/impulse_dump/orderflow_impulse/knife_reversal/cascade_hunter/strongest_coin remains open under virtual TP/SL monitor",
                            "quick_bounce_virtual_since": time.time(),
                            "impulse_dump_virtual_since": time.time() if strategy_name == "impulse_dump" else 0,
                            "orderflow_impulse_virtual_since": time.time() if strategy_name in {"orderflow_impulse", "knife_reversal", "cascade_hunter", "strongest_coin"} else 0,
                            "quick_bounce_real_tpsl_error": str(protection.get("tpsl_error") or protection.get("tp_error") or protection.get("sl_error") or protection.get("verify_error") or protection.get("protection_warning") or "")[:700],
                            "impulse_dump_real_tpsl_error": str(protection.get("tpsl_error") or protection.get("tp_error") or protection.get("sl_error") or protection.get("verify_error") or protection.get("protection_warning") or "")[:700] if strategy_name == "impulse_dump" else "",
                            "orderflow_impulse_real_tpsl_error": str(protection.get("tpsl_error") or protection.get("tp_error") or protection.get("sl_error") or protection.get("verify_error") or protection.get("protection_warning") or "")[:700] if strategy_name in {"orderflow_impulse", "knife_reversal", "cascade_hunter", "strongest_coin"} else "",
                        })
                        pos.update(protection)
                        pos["updated_at"] = time.time()
                        await self.storage.upsert_position(pos)
                        try:
                            from debug_log import log_event
                            log_event(
                                f"{strategy_name}_virtual_protection",
                                stage="protection",
                                ok=True,
                                symbol=pos.get("symbol"),
                                side=pos.get("side"),
                                take_price=pos.get("take_price"),
                                stop_price=pos.get("stop_price"),
                                real_tpsl_failed=True,
                                reason=protection.get("quick_bounce_real_tpsl_error") or protection.get("impulse_dump_real_tpsl_error") or protection.get("orderflow_impulse_real_tpsl_error"),
                            )
                        except Exception:
                            pass
                    else:
                        # v0148 hard rule for non-BOOST live scalping: no confirmed exchange TP/SL
                        # means no position.  BOOST, Quick Bounce, Impulse Dump and Orderflow Impulse are exempt above.
                        reason = "protection_failed_no_exchange_tpsl"
                        if bool(pos.get("liquidation_stop_mode")):
                            reason = "protection_failed_no_exchange_tp_liq_mode"
                        pos["protection_mode"] = "closing_unprotected"
                        pos["protection_warning"] = "exchange protection missing after retries; closing position"
                        pos["updated_at"] = time.time()
                        await self.storage.upsert_position(pos)
                        close_res = await self.close_position(pos, reason=reason, live=True)
                        return {"ok": False, "order": order, "position": pos, "reason": "exchange protection missing; position closed", "protection": protection, "close": close_res}
            return {"ok": True, "order": order, "position": pos}


    def _sanitize_position_for_exchange(self, pos: dict) -> dict:
        """Round qty/TP/SL once and keep local display equal to what MEXC receives."""
        try:
            if hasattr(self.exchange_client, "sanitize_protection_values"):
                vals = self.exchange_client.sanitize_protection_values(
                    pos.get("symbol"),
                    float(pos.get("qty") or 0),
                    pos.get("stop_price"),
                    pos.get("take_price"),
                )
                for k, v in vals.items():
                    if v not in (None, ""):
                        pos[k] = v
        except Exception as e:
            pos["precision_warning"] = str(e)[:160]
        return pos


    def exchange_position_qty(self, pos: dict) -> float:
        """Return base-coin amount suitable for create_order(amount=...).

        Native MEXC position sync exposes both `contracts` and `amount`; MEXC
        close orders in this bot accept base amount and convert it back to
        contract volume. Prefer `amount`; otherwise convert contracts by
        contractSize when available.
        """
        info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
        for key in ("amount", "qty", "size"):
            try:
                value = pos.get(key)
                if value not in (None, ""):
                    return abs(float(value))
            except Exception:
                pass
        contracts = pos.get("contracts")
        if contracts is None:
            contracts = info.get("positionAmt") or info.get("holdVol") or info.get("vol")
        try:
            contracts_f = abs(float(contracts or 0))
            cs = pos.get("contractSize")
            if cs is None:
                cs = info.get("contractSize") or info.get("contract_size")
            cs_f = float(cs or 0)
            return contracts_f * cs_f if cs_f > 0 else contracts_f
        except Exception:
            return 0.0

    def _exchange_position_to_close_order(self, pos: dict) -> dict:
        info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
        symbol = pos.get("symbol") or info.get("symbol")
        contracts = pos.get("contracts", pos.get("contractSize", pos.get("amount", pos.get("size"))))
        if contracts is None:
            contracts = info.get("positionAmt") or info.get("holdVol") or info.get("vol")
        qty = self.exchange_position_qty(pos)
        side_raw = str(pos.get("side") or info.get("side") or "").lower()
        if not symbol or qty <= 0:
            return {"skip": True, "reason": "empty position"}
        if "short" in side_raw or side_raw in {"sell", "2", "3"}:
            side = "buy"
        elif "long" in side_raw or side_raw in {"buy", "1"}:
            side = "sell"
        else:
            # Some venues expose signed size instead of side. Negative means short.
            signed = float(contracts or 0)
            side = "buy" if signed < 0 else "sell"
        return {"symbol": symbol, "qty": qty, "side": side}


    def _position_symbol_matches(self, row: dict, symbol: str) -> bool:
        """Return True when an exchange position row belongs to the local symbol."""
        info = row.get("info", {}) if isinstance(row.get("info"), dict) else {}
        row_symbol = str(row.get("symbol") or info.get("symbol") or "")
        if not row_symbol or not symbol:
            return False
        if row_symbol == symbol:
            return True
        try:
            return bool(self.exchange_client._mexc_variants_match(row_symbol, symbol))
        except Exception:
            return row_symbol.replace(":USDT", "").replace("_", "/") == symbol.replace(":USDT", "").replace("_", "/")

    def _exchange_row_contracts(self, row: dict) -> float:
        """Return real open contract/volume count from a position row.

        Never use contractSize here: contractSize is instrument metadata, not an
        open position amount. Using it made close confirmation think an already
        closed MEXC position was still open.
        """
        info = row.get("info", {}) if isinstance(row.get("info"), dict) else {}
        for key in ("contracts", "qty", "amount", "size"):
            try:
                value = row.get(key)
                if value not in (None, ""):
                    return abs(float(value))
            except Exception:
                pass
        for key in ("holdVol", "vol", "positionAmt"):
            try:
                value = info.get(key)
                if value not in (None, ""):
                    return abs(float(value))
            except Exception:
                pass
        return 0.0

    async def _find_exchange_position_row(self, symbol: str) -> dict | None:
        """Fetch the current real exchange position for symbol, if any."""
        if not hasattr(self.exchange_client, "fetch_positions"):
            return None
        rows = []
        try:
            rows = await self.exchange_client.fetch_positions([symbol])
        except TypeError:
            try:
                rows = await self.exchange_client.fetch_positions()
            except Exception:
                rows = []
        except Exception:
            rows = []
        for row in rows or []:
            if self._position_symbol_matches(row, symbol) and self._exchange_row_contracts(row) > 0:
                return row
        return None

    async def close_exchange_position(self, pos: dict, reason: str = "external_close") -> dict:
        # Prefer the native MEXC close-by-position row, because exchange-only
        # positions from /position/open_positions contain exact holdVol and
        # positionType. This is more reliable than ccxt reduceOnly for MEXC.
        try:
            if hasattr(self.exchange_client, "mexc_close_position_market_native"):
                res = await self.exchange_client.mexc_close_position_market_native(pos)
                return {"ok": True, "order": res, "native_mexc_close": True}
        except Exception as native_error:
            native_reason = str(native_error)
        else:
            native_reason = ""
        order = self._exchange_position_to_close_order(pos)
        if order.get("skip"):
            return {"ok": True, "skipped": True, "reason": order.get("reason")}
        try:
            res = await self._create_order_retry(
                order["symbol"], "market", order["side"], order["qty"], None,
                {"reduceOnly": True, "clientOrderId": f"bot_{reason}_{int(time.time()*1000)}"}, attempts=2
            )
            return {"ok": True, "order": res, "native_mexc_error": native_reason}
        except Exception as e:
            return {"ok": False, "reason": str(e), "native_mexc_error": native_reason}

    def _close_order_side_for_position(self, side: str) -> str:
        """Return order side needed to close a position side.

        Internal positions store side as LONG/SHORT, while MEXC native plan
        orders expect close order side as buy/sell. Passing LONG/SHORT into the
        native TP/SL layer made triggerType fallback to a wrong default and could
        make SL/TP never place correctly.
        """
        s = str(side or "").strip().lower()
        if s in {"long", "buy"}:
            return "sell"
        if s in {"short", "sell"}:
            return "buy"
        return s

    async def _stored_position_leverage(self, symbol: str) -> int | None:
        try:
            for p in await self.storage.positions():
                if str(p.get("symbol") or "") == str(symbol or ""):
                    lev = int(float(p.get("leverage") or 0))
                    return lev if lev > 0 else None
        except Exception:
            return None
        return None

    async def _create_trigger_market_order(self, symbol: str, side: str, qty: float, trigger_price: float, kind: str) -> dict:
        errors = []
        close_side = self._close_order_side_for_position(side)
        try:
            native_name = "mexc_place_take_profit_market" if kind == "tp" else "mexc_place_stop_market"
            if hasattr(self.exchange_client, native_name):
                fn = getattr(self.exchange_client, native_name)
                lev = await self._stored_position_leverage(symbol)
                return await fn(
                    symbol=symbol, close_side=close_side, amount=qty, trigger_price=trigger_price,
                    client_order_id=f"bot_{kind}_{int(time.time()*1000)}", leverage=lev,
                )
        except Exception as e:
            errors.append(f"native_{kind}_plan: {e}")
        attempts = [
            ("market", None, {"reduceOnly": True, "stopPrice": trigger_price, "triggerPrice": trigger_price, "clientOrderId": f"bot_{kind}_{int(time.time()*1000)}"}),
            ("market", None, {"reduceOnly": True, f"{'takeProfitPrice' if kind == 'tp' else 'stopLossPrice'}": trigger_price, "clientOrderId": f"bot_{kind}_{int(time.time()*1000)}"}),
            ("stop_market", None, {"reduceOnly": True, "stopPrice": trigger_price, "triggerPrice": trigger_price, "clientOrderId": f"bot_{kind}_{int(time.time()*1000)}"}),
        ]
        for type_, price, params in attempts:
            try:
                return await self.exchange_client.create_order(symbol, type_, close_side, qty, price, params)
            except Exception as e:
                errors.append(f"{type_}: {e}")
        raise RuntimeError(f"{kind}-market protection failed: " + " | ".join(errors))

    async def _create_stop_market_order(self, symbol: str, side: str, qty: float, stop_price: float) -> dict:
        return await self._create_trigger_market_order(symbol, side, qty, stop_price, "sl")

    async def _create_take_profit_market_order(self, symbol: str, side: str, qty: float, take_price: float) -> dict:
        return await self._create_trigger_market_order(symbol, side, qty, take_price, "tp")

    async def _create_cascade_split_tp_orders(self, pos: dict, close_side: str, qty: float) -> dict:
        """CASCADE HUNTER uses two non-trailing take-profits: TP1 50% at 1R, TP2 rest at 2R."""
        tp1 = float(pos.get("partial_take_price") or 0)
        tp2 = float(pos.get("final_take_price") or pos.get("take_price") or 0)
        frac = max(0.01, min(0.99, float(pos.get("partial_take_fraction") or 0.50)))
        qty1 = max(0.0, qty * frac)
        qty2 = max(0.0, qty - qty1)
        out = {"tp1_price": tp1, "tp2_price": tp2, "tp1_fraction": frac, "tp1_qty": qty1, "tp2_qty": qty2}
        if tp1 <= 0 or tp2 <= 0 or qty1 <= 0 or qty2 <= 0:
            out["tp_error"] = "missing cascade split TP price/qty"
            return out
        o1 = await self._create_take_profit_market_order(pos["symbol"], close_side, qty1, tp1)
        o2 = await self._create_take_profit_market_order(pos["symbol"], close_side, qty2, tp2)
        out.update({
            "tp1_order_id": o1.get("id"),
            "tp2_order_id": o2.get("id"),
            "tp_order_id": ",".join([x for x in [str(o1.get("id") or ""), str(o2.get("id") or "")] if x]),
            "tp_raw": {"tp1": o1, "tp2": o2},
        })
        return out

    async def _verify_exchange_protection(self, pos: dict, tp_order_id: str = "", sl_order_id: str = "") -> dict:
        """Confirm that both TP and SL are actually visible on exchange.

        MEXC may accept a request but return an empty id or expose the order only
        through plan/stop endpoints. For live scalping, trusting the create
        response is unsafe, so every protection placement is confirmed with
        fetch_open_orders(), which already merges normal + plan + stop/TP-SL
        endpoints in exchange_client.
        """
        try:
            # Unit-test/non-MEXC adapters often cannot expose native plan/stop
            # endpoints. The strict exchange-first verification is required for
            # MEXC live futures; for generic adapters an id from both create calls
            # is the best available confirmation.
            if str(getattr(self.exchange_client, "exchange_id", "") or "").lower() != "mexc" and tp_order_id and sl_order_id:
                return {"tp_exists": True, "sl_exists": True, "protection_status": "EXCHANGE PROTECTED", "protection_mode": "exchange"}
            from protection_engine import ProtectionEngine
            check_pos = {**dict(pos), "tp_order_id": tp_order_id, "sl_order_id": sl_order_id}
            pe = ProtectionEngine(self.exchange_client)
            # v0232: MEXC /planorder/place can return success + order ids while
            # /stoporder/open_orders remains empty because these are plan orders,
            # not stoporder rows. Verify the exact planorder ids directly before
            # falling back to generic open-order classification. This prevents
            # Quick Bounce from falsely downgrading real TP/SL to virtual mode.
            if (
                str(getattr(self.exchange_client, "exchange_id", "") or "").lower() == "mexc"
                and hasattr(self.exchange_client, "mexc_find_active_plan_order")
                and tp_order_id
                and sl_order_id
            ):
                try:
                    tp_row = await self.exchange_client.mexc_find_active_plan_order(check_pos.get("symbol"), order_id=tp_order_id)
                    sl_row = await self.exchange_client.mexc_find_active_plan_order(check_pos.get("symbol"), order_id=sl_order_id)
                    if tp_row and sl_row:
                        return {
                            "tp_exists": True,
                            "sl_exists": True,
                            "take_profit_ok": True,
                            "stop_loss_ok": True,
                            "tp_order_id": tp_order_id,
                            "sl_order_id": sl_order_id,
                            "protection_status": "EXCHANGE PROTECTED",
                            "protection_mode": "exchange_planorder",
                            "protection_note": "MEXC planorder TP/SL verified by id",
                        }
                except Exception as plan_verify_error:
                    check_pos["planorder_verify_warning"] = str(plan_verify_error)[:240]
            # BOOST/HUNTER emergency-SL-only planorders must be verified through
            # ProtectionEngine.check(), because MEXC exposes them under
            # /planorder/list/orders and not necessarily under stoporder/* logs.
            if str(check_pos.get("strategy") or "").lower() == "boost_scalping" or str(check_pos.get("tp_order_id") or "") == "LIVE_TRAILING_NO_FIXED_TP" or bool(check_pos.get("boost_emergency_sl_only")):
                return await pe.check(check_pos)
            orders = await self.exchange_client.fetch_open_orders(check_pos.get("symbol"))
            return pe.classify_orders(check_pos, orders or [])
        except Exception as e:
            return {
                "tp_exists": False,
                "sl_exists": False,
                "protection_status": "LOCAL BOT PROTECTED",
                "protection_mode": "local_monitoring",
                "verify_error": str(e)[:240],
            }

    async def place_protection_orders(self, pos: dict, live: bool) -> dict:
        if not live:
            return {}
        pos = await self._normalize_protection_distance(self._sanitize_position_for_exchange(dict(pos)))
        symbol = pos["symbol"]
        # Do not place protection until the exchange exposes positionId/holdVol.
        live_row = await self._wait_for_live_position(symbol, pos.get("side"))
        if live_row:
            try:
                pos["qty"] = self.exchange_position_qty(live_row) or pos.get("qty")
                pos["exchange_contracts"] = live_row.get("contracts")
                pos["raw_exchange_position"] = live_row
                info = live_row.get("info", {}) if isinstance(live_row.get("info"), dict) else {}
                for key in ("entryPrice", "entry_price", "average"):
                    val = live_row.get(key)
                    if val and float(val) > 0:
                        pos["entry_price"] = float(val); break
                if not float(pos.get("entry_price") or 0):
                    for key in ("holdAvgPrice", "openAvgPrice", "entryPrice"):
                        val = info.get(key)
                        if val and float(val) > 0:
                            pos["entry_price"] = float(val); break
                pos = await self._normalize_protection_distance(pos)
            except Exception as e:
                pos["protection_position_sync_warning"] = str(e)[:180]
        symbol = pos["symbol"]
        qty = float(pos.get("qty") or 0)
        liquidation_stop_mode = bool(pos.get("liquidation_stop_mode")) and str(pos.get("strategy") or "").lower() == "ai_scalping"
        if qty <= 0:
            return {"ok": False, "protection_status": "LOCAL BOT PROTECTED", "protection_mode": "local_monitoring", "protection_warning": "missing qty for exchange protection"}
        side = "sell" if str(pos.get("side")).upper() == "LONG" else "buy"
        strategy_name_for_protection = str(pos.get("strategy") or "").lower()
        require_exchange_protection = await self._setting_bool("require_exchange_protection", "REQUIRE_EXCHANGE_PROTECTION", True)
        attempts = max(1, int(os.getenv("PROTECTION_PLACE_MAX_ATTEMPTS", "3") or "3"))
        delay = float(os.getenv("PROTECTION_RECHECK_DELAY_SEC", "0.8") or "0.8")

        # v0147 robust flow:
        # 1) entry order may already have native MEXC TP/SL attached; verify it first.
        # 2) only if missing, attach TP/SL to the now-live position.
        # 3) only if still missing, create separate trigger-market fallback legs.
        try:
            existing = await self._verify_exchange_protection(pos, str(pos.get("tp_order_id") or ""), str(pos.get("sl_order_id") or ""))
            if liquidation_stop_mode:
                if existing.get("tp_exists"):
                    return {**existing, "ok": True, "sl_exists": True, "sl_order_id": "LIQUIDATION_STOP", "protection_status": "TP + LIQUIDATION STOP", "protection_mode": "exchange_tp_liquidation_sl", "protection_note": "existing TP detected before reattach"}
            elif existing.get("tp_exists") and existing.get("sl_exists"):
                if strategy_name_for_protection == "boost_scalping" or existing.get("protection_mode") == "exchange_emergency_sl_only":
                    return {**existing, "ok": True, "protection_status": "EMERGENCY SL ONLY", "protection_mode": "exchange_emergency_sl_only", "protection_note": "existing BOOST emergency SL detected before reattach"}
                return {**existing, "ok": True, "protection_status": "EXCHANGE PROTECTED", "protection_mode": "exchange", "protection_note": "existing TP/SL detected before reattach"}
        except Exception as e:
            pos["pre_protection_verify_warning"] = str(e)[:180]
        if strategy_name_for_protection in {"ai_scalping", "boost_scalping"}:
            # AI BTC/ETH scalping and BOOST both use tiny local exits. Exchange
            # TP/SL is safety/backstop and MEXC often needs more retries before
            # native stoporder rows become visible. BOOST now gets the same
            # protection retry logic as BTC/ETH scalping.
            attempts_env = "BOOST_PROTECTION_ATTEMPTS" if strategy_name_for_protection == "boost_scalping" else "AI_SCALPING_PROTECTION_ATTEMPTS"
            delay_env = "BOOST_PROTECTION_RECHECK_DELAY_SEC" if strategy_name_for_protection == "boost_scalping" else "AI_SCALPING_PROTECTION_RECHECK_DELAY_SEC"
            attempts = max(attempts, int(os.getenv(attempts_env, os.getenv("AI_SCALPING_PROTECTION_ATTEMPTS", "5")) or "5"))
            delay = max(delay, float(os.getenv(delay_env, os.getenv("AI_SCALPING_PROTECTION_RECHECK_DELAY_SEC", "1.2")) or "1.2"))
        history = []
        best = {"ok": False}
        for i in range(attempts):
            tp = float(pos.get("take_price") or 0)
            sl = float(pos.get("stop_price") or 0)
            out = {"ok": True, "attempt": i + 1}
            log_event("protection_attempt_start", symbol=symbol, side=side, qty=qty, take_price=tp, stop_price=sl, liquidation_stop_mode=liquidation_stop_mode, attempt=i + 1)
            # Do NOT cancel all orders on early retries. In v0146 this could
            # remove an attached TP/SL leg that was actually accepted but not yet
            # visible in every endpoint. Only do destructive cleanup after several
            # failed attempts, and only when explicitly enabled.
            if (
                strategy_name_for_protection not in {"boost_scalping", "quick_bounce", "impulse_dump", "orderflow_impulse", "knife_reversal", "cascade_hunter", "strongest_coin"}
                and i >= 2
                and os.getenv("PROTECTION_CANCEL_STALE_ON_RETRY", "false").lower() in {"1", "true", "yes", "on"}
                and hasattr(self.exchange_client, "cancel_all_orders")
            ):
                try:
                    await self.exchange_client.cancel_all_orders(symbol)
                except Exception as e:
                    out["retry_cancel_error"] = str(e)[:240]
                await asyncio.sleep(delay)
            elif strategy_name_for_protection in {"boost_scalping", "quick_bounce", "impulse_dump", "orderflow_impulse", "knife_reversal", "cascade_hunter", "strongest_coin"} and i >= 2:
                out["retry_cancel_skipped"] = "fast strategy protection: never cancel possible existing planorder backstop"
            if liquidation_stop_mode:
                try:
                    if tp > 0:
                        order = await self._create_take_profit_market_order(symbol, side, qty, tp)
                        out["tp_order_id"] = order.get("id")
                        out["tp_raw"] = order
                    else:
                        out["tp_error"] = "missing take_price"
                    out["sl_exists"] = True
                    out["sl_order_id"] = "LIQUIDATION_STOP"
                    out["liquidation_stop_mode"] = True
                except Exception as e:
                    out["tp_error"] = str(e)[:500]
            elif strategy_name_for_protection == "boost_scalping" and self._truthy(await self._setting("boost_emergency_sl_only", os.getenv("BOOST_EMERGENCY_SL_ONLY", "true")), True):
                # HUNTER BOOST: exchange protection is emergency SL only.
                # No fixed TP is placed on the exchange; live trailing/momentum decay exits
                # the position in profit. This avoids old planorder TP/SL fallback problems
                # and prevents the bot from harvesting tiny live losses.
                try:
                    if sl > 0:
                        log_event("boost_emergency_sl_request", symbol=symbol, side=side, qty=qty, stop_price=sl, attempt=i + 1)
                        order = await self._create_stop_market_order(symbol, side, qty, sl)
                        out["sl_order_id"] = order.get("id")
                        out["sl_raw"] = order
                        out["sl_exists"] = True
                        out["tp_exists"] = True
                        out["tp_order_id"] = "LIVE_TRAILING_NO_FIXED_TP"
                        out["protection_mode"] = "exchange_emergency_sl_only"
                        out["protection_status"] = "EMERGENCY SL ONLY"
                        out["protection_note"] = "HUNTER live exit manages profit; exchange has emergency SL only"
                        out["ok"] = True
                        log_event("boost_emergency_sl_response", symbol=symbol, side=side, stop_price=sl, order=order, ok=True)
                    else:
                        out["sl_error"] = "missing stop_price"
                except Exception as e:
                    out["sl_error"] = str(e)[:800]
                    log_event("error_boost_emergency_sl", symbol=symbol, side=side, qty=qty, stop_price=sl, attempt=i + 1, error=str(e), ok=False)
            elif strategy_name_for_protection in {"cascade_hunter", "strongest_coin"}:
                # Split TP mode: TP1 closes 50% at 1R, TP2 closes the remaining 50% at 2R.
                # Do not use MEXC native by-position TP/SL here because it supports one TP only.
                try:
                    split = await self._create_cascade_split_tp_orders(pos, side, qty)
                    out.update(split)
                    if split.get("tp_error"):
                        out["tp_error"] = split.get("tp_error")
                except Exception as e:
                    out["tp_error"] = str(e)[:800]
                    log_event("error_split_tp", symbol=symbol, side=side, qty=qty, tp1=pos.get("partial_take_price"), tp2=pos.get("take_price"), attempt=i + 1, error=str(e), ok=False)
                try:
                    if sl > 0:
                        log_event("split_tp_sl_request", symbol=symbol, side=side, qty=qty, stop_price=sl, attempt=i + 1)
                        order = await self._create_stop_market_order(symbol, side, qty, sl)
                        out["sl_order_id"] = order.get("id")
                        out["sl_raw"] = order
                    else:
                        out["sl_error"] = "missing stop_price"
                except Exception as e:
                    out["sl_error"] = str(e)[:800]
                    log_event("error_cascade_split_sl", symbol=symbol, side=side, qty=qty, trigger_price=sl, attempt=i + 1, error=str(e), ok=False)

            elif str(getattr(self.exchange_client, "exchange_id", "") or "").lower() == "mexc" and hasattr(self.exchange_client, "mexc_place_tpsl_by_position"):
                # v0160: MEXC native by-position TP/SL FIRST, using confirmed live position row.
                # Use /api/v1/private/stoporder/place with positionId + holdVol;
                # standalone plan orders remain only as fallback.
                native_ok = False
                try:
                    if tp <= 0 or sl <= 0:
                        raise RuntimeError("missing take_price/stop_price for native position TP/SL")
                    log_event("mexc_native_tpsl_request", symbol=symbol, side=side, qty=qty, stop_price=sl, take_price=tp, attempt=i + 1)
                    order = await self.exchange_client.mexc_place_tpsl_by_position(
                        symbol=symbol, side=side, qty=qty, stop_price=sl, take_price=tp,
                        client_order_id=f"bot_tpsl_{int(time.time()*1000)}",
                        live_position=live_row,
                    )
                    log_event("mexc_native_tpsl_response", symbol=symbol, side=side, attempt=i + 1, order=order, ok=True)
                    oid = str(order.get("id") or "")
                    if oid:
                        out["tp_order_id"] = oid
                        out["sl_order_id"] = oid
                    out["tpsl_raw"] = order
                    native_ok = True
                except Exception as e:
                    if self._is_mexc_tpsl_already_exists_error(e):
                        out["tp_order_id"] = "MEXC_TPSL_ALREADY_EXISTS"
                        out["sl_order_id"] = "MEXC_TPSL_ALREADY_EXISTS"
                        out["tp_exists"] = True
                        out["sl_exists"] = True
                        out["tpsl_already_exists"] = True
                        out["tpsl_note"] = "MEXC says native position TP/SL already exists; treating as protected"
                        native_ok = True
                    else:
                        out["tpsl_error"] = str(e)[:800]
                        log_event("error_mexc_native_tpsl", symbol=symbol, side=side, qty=qty, stop_price=sl, take_price=tp, attempt=i + 1, error=str(e), ok=False)

                if not native_ok and strategy_name_for_protection == "boost_scalping":
                    out["tp_error"] = out.get("tp_error") or "BOOST native position TP/SL failed; standalone planorder fallback disabled to prevent instant wrong-side loss"
                    out["sl_error"] = out.get("sl_error") or "BOOST native position TP/SL failed; local positive-exit monitor only"
                    log_event("boost_planorder_fallback_disabled", symbol=symbol, side=side, qty=qty, take_price=tp, stop_price=sl, attempt=i + 1, ok=False, reason="native_tpsl_failed_no_standalone_planorder")
                elif not native_ok:
                    try:
                        if tp > 0:
                            log_event("mexc_trigger_tp_request", symbol=symbol, side=side, qty=qty, trigger_price=tp, attempt=i + 1)
                            order = await self._create_take_profit_market_order(symbol, side, qty, tp)
                            out["tp_order_id"] = order.get("id")
                            out["tp_raw"] = order
                            log_event("mexc_trigger_tp_response", symbol=symbol, side=side, trigger_price=tp, attempt=i + 1, order=order, ok=True)
                        else:
                            out["tp_error"] = "missing take_price"
                    except Exception as e:
                        out["tp_error"] = str(e)[:800]
                        log_event("error_mexc_trigger_tp", symbol=symbol, side=side, qty=qty, trigger_price=tp, attempt=i + 1, error=str(e), ok=False)
                    try:
                        if sl > 0:
                            log_event("mexc_trigger_sl_request", symbol=symbol, side=side, qty=qty, trigger_price=sl, attempt=i + 1)
                            order = await self._create_stop_market_order(symbol, side, qty, sl)
                            out["sl_order_id"] = order.get("id")
                            out["sl_raw"] = order
                            log_event("mexc_trigger_sl_response", symbol=symbol, side=side, trigger_price=sl, attempt=i + 1, order=order, ok=True)
                        else:
                            out["sl_error"] = "missing stop_price"
                    except Exception as e:
                        out["sl_error"] = str(e)[:800]
                        log_event("error_mexc_trigger_sl", symbol=symbol, side=side, qty=qty, trigger_price=sl, attempt=i + 1, error=str(e), ok=False)

            else:
                try:
                    if tp > 0:
                        order = await self._create_take_profit_market_order(symbol, side, qty, tp)
                        out["tp_order_id"] = order.get("id")
                        out["tp_raw"] = order
                    else:
                        out["tp_error"] = "missing take_price"
                except Exception as e:
                    out["tp_error"] = str(e)[:500]
                try:
                    if sl > 0:
                        order = await self._create_stop_market_order(symbol, side, qty, sl)
                        out["sl_order_id"] = order.get("id")
                        out["sl_raw"] = order
                    else:
                        out["sl_error"] = "missing stop_price"
                except Exception as e:
                    out["sl_error"] = str(e)[:500]
            await asyncio.sleep(delay)
            if liquidation_stop_mode:
                # In liquidation-stop mode the planned SL is intentionally NOT
                # placed on the exchange.  Only TP must be confirmed.  Do not let
                # the generic TP+SL protection checker reject a valid liq-stop
                # position just because there is no SL order.
                verified = await self._verify_exchange_protection(pos, str(out.get("tp_order_id") or ""), "")
                out.update({k: v for k, v in verified.items() if k not in {"tp_order_id", "sl_order_id"} or v})
                out["sl_exists"] = True
                out["sl_order_id"] = "LIQUIDATION_STOP"
                out["ok"] = bool(out.get("tp_exists"))
                out["protection_status"] = "TP + LIQUIDATION STOP" if out["ok"] else "LOCAL BOT PROTECTED"
                out["protection_mode"] = "exchange_tp_liquidation_sl" if out["ok"] else "local_monitoring"
            else:
                if strategy_name_for_protection == "boost_scalping" and self._truthy(await self._setting("boost_emergency_sl_only", os.getenv("BOOST_EMERGENCY_SL_ONLY", "true")), True) and out.get("sl_order_id"):
                    # Only SL has to exist in HUNTER mode. Do not require a TP order.
                    out["tp_exists"] = True
                    out["tp_order_id"] = out.get("tp_order_id") or "LIVE_TRAILING_NO_FIXED_TP"
                    out["sl_exists"] = True
                    out["ok"] = True
                    out["protection_status"] = "EMERGENCY SL ONLY"
                    out["protection_mode"] = "exchange_emergency_sl_only"
                    out["boost_unsafe_position"] = False
                    out["boost_defensive_mode"] = False
                elif strategy_name_for_protection in {"cascade_hunter", "strongest_coin"} and out.get("tp1_order_id") and out.get("tp2_order_id") and out.get("sl_order_id"):
                    out["tp_exists"] = True
                    out["sl_exists"] = True
                    out["take_profit_ok"] = True
                    out["stop_loss_ok"] = True
                    out["ok"] = True
                    out["protection_status"] = "EXCHANGE PROTECTED"
                    out["protection_mode"] = "exchange_split_tp"
                    out["protection_note"] = "Split TP verified by returned planorder ids: TP1 50% at 1R, TP2 rest at 2R"
                else:
                    verified = await self._verify_exchange_protection(pos, str(out.get("tp_order_id") or ""), str(out.get("sl_order_id") or ""))
                    out.update({k: v for k, v in verified.items() if k not in {"tp_order_id", "sl_order_id"} or v})
                    # v0148 strict exchange-first check: a returned id is not enough.
                    # TP/SL must be visible through MEXC open/plan/stop/TP-SL endpoints.
                    out["ok"] = bool(out.get("tp_exists") and out.get("sl_exists"))
                    out["protection_status"] = "EXCHANGE PROTECTED" if out["ok"] else "LOCAL BOT PROTECTED"
                    out["protection_mode"] = "exchange" if out["ok"] else "local_monitoring"
            log_event("protection_attempt_result", symbol=symbol, side=side, attempt=i + 1, ok=out.get("ok"), tp_exists=out.get("tp_exists"), sl_exists=out.get("sl_exists"), tp_order_id=out.get("tp_order_id"), sl_order_id=out.get("sl_order_id"), tpsl_error=out.get("tpsl_error"), tp_error=out.get("tp_error"), sl_error=out.get("sl_error"), verify_error=out.get("verify_error"))
            history.append({k: v for k, v in out.items() if k not in {"tp_raw", "sl_raw"}})
            best = out
            if out["ok"]:
                out["protection_attempts"] = history
                out.pop("protection_warning", None)
                return out
        best["ok"] = not require_exchange_protection
        best["protection_status"] = "LOCAL BOT PROTECTED"
        best["protection_mode"] = "local_monitoring"
        best["protection_attempts"] = history
        best["protection_warning"] = "exchange protection not confirmed after attempts"
        if strategy_name_for_protection == "boost_scalping" and self._truthy(await self._setting("boost_emergency_sl_only", os.getenv("BOOST_EMERGENCY_SL_ONLY", "true")), True):
            # v0209: HUNTER UNSAFE state. Entry may already be live, but the exchange
            # emergency SL is missing. Do not pretend this is normal local protection:
            # the BOOST loop will switch to defensive mode, retry the SL, disable
            # aggressive rotation/rescue, and exit faster only when real profit exists.
            best["ok"] = False
            best["boost_unsafe_position"] = True
            best["boost_defensive_mode"] = True
            best["protection_status"] = "UNSAFE POSITION"
            best["protection_mode"] = "unsafe_no_emergency_sl"
            best["protection_warning"] = "BOOST emergency exchange SL failed; defensive live monitoring enabled"
            best["boost_unsafe_reason"] = str(best.get("sl_error") or best.get("verify_error") or "emergency SL not confirmed")[:500]
        log_event("error_protection_failed_final", symbol=symbol, side=side, qty=qty, take_price=pos.get("take_price"), stop_price=pos.get("stop_price"), attempts=history, ok=False, boost_unsafe=best.get("boost_unsafe_position", False))
        return best

    async def cancel_entry(self, pos: dict, live: bool, reason: str = "limit_timeout"):
        symbol = pos["symbol"]
        if live and pos.get("order_id"):
            try:
                await self.exchange_client.cancel_order(pos["order_id"], symbol)
            except Exception as e:
                pos["cancel_error"] = str(e)
                await self.storage.upsert_position(pos)
        await self.storage.remove_position(symbol)
        await self.storage.set_lock(symbol, 30, reason)
        return {"ok": True, "reason": reason}


    async def _close_until_flat(self, symbol: str, reason: str, side: str, fallback_qty: float) -> tuple[bool, list]:
        """Close a futures position and keep retrying small exchange tails.

        MEXC can leave a residual contract after a market close because local
        qty is base amount while native close requires integer holdVol. Every
        retry fetches the real exchange row and closes its current holdVol, so
        local rounding cannot create partial closes.
        """
        attempts = max(1, int(os.getenv("POST_CLOSE_MAX_ATTEMPTS", "4") or "4"))
        delay = float(os.getenv("POST_CLOSE_POSITION_RECHECK_DELAY_SEC", os.getenv("POST_CLOSE_BALANCE_CHECK_DELAY_SEC", "1.5")))
        results = []
        for i in range(attempts):
            exchange_row = await self._find_exchange_position_row(symbol)
            if exchange_row:
                res = await self.close_exchange_position(exchange_row, reason=reason)
                results.append({"attempt": i + 1, "mode": "exchange_row", "result": res})
                if not res.get("ok"):
                    return False, results
            elif i == 0 and fallback_qty > 0:
                # v0232: if the exchange already reports no live position, do not
                # send a blind reduce-only fallback close. On MEXC this produced
                # noisy code 2009 logs ("Position is nonexistent or closed") right
                # after real TP/SL had already closed the trade. Treat exchange-flat
                # as idempotent success; hidden-margin recovery is handled by the
                # separate safe-state/recovery checks, not by guessing a close side.
                results.append({"attempt": i + 1, "mode": "already_flat_no_exchange_row", "result": {"ok": True, "skipped": True, "reason": "exchange reports no open position"}})
                return True, results
            else:
                return True, results
            await asyncio.sleep(delay)
            if not await self._find_exchange_position_row(symbol):
                return True, results
        return (not await self._find_exchange_position_row(symbol)), results


    async def _mexc_hidden_margin_after_close(self) -> tuple[bool, dict]:
        """Return True when MEXC balance still shows live position margin.

        MEXC can occasionally return an empty open_positions list immediately
        after a close attempt while account/assets still shows used/position
        margin. In that case we must NOT delete local position state, because
        the exchange may still have live exposure that /positions cannot see.
        """
        try:
            bal = await self.exchange_client.fetch_balance()
            usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
            used = float(usdt.get("used") or ((bal or {}).get("used", {}) or {}).get("USDT") or 0)
            pm = float(usdt.get("positionMargin") or usdt.get("position_margin") or 0)
            upnl = float(usdt.get("unrealized") or 0)
            hidden = used > 0.5 or pm > 0.5 or abs(upnl) > 0.01
            return hidden, {"used": used, "positionMargin": pm, "unrealized": upnl}
        except Exception as e:
            return False, {"error": str(e)[:180]}

    async def _balance_cash_usdt(self) -> float | None:
        """Best-effort futures cash balance for realized live PnL deltas.

        For live BOOST scalping, local mark-price PnL can differ materially from
        MEXC execution because TP is only 0.03-0.05%.  cashBalance is the most
        useful value for realized close delta; fall back to total only if cash is
        unavailable.
        """
        try:
            bal = await self.exchange_client.fetch_balance()
            usdt = (bal or {}).get("USDT", {}) if isinstance(bal, dict) else {}
            for key in ("cashBalance", "availableCash", "total", "free"):
                try:
                    v = usdt.get(key) if isinstance(usdt, dict) else None
                    if v not in (None, ""):
                        return float(v)
                except Exception:
                    pass
            for section in ("total", "free"):
                try:
                    v = ((bal or {}).get(section, {}) or {}).get("USDT")
                    if v not in (None, ""):
                        return float(v)
                except Exception:
                    pass
        except Exception:
            return None
        return None

    async def close_position(self, pos: dict, reason: str, live: bool, exit_price: float | None = None):
        symbol = pos["symbol"]
        side = "sell" if str(pos.get("side")).upper() == "LONG" else "buy"
        qty = float(pos.get("qty") or 0)
        pos["status"] = "closing"
        await self.storage.upsert_position(pos)
        already_closed = False
        close_attempts = []
        strategy = str(pos.get("strategy") or "").lower()
        live_boost = bool(live and strategy == "boost_scalping")
        # v0225: take a real futures cash snapshot for every LIVE close, not only
        # BOOST. Local mark-price PnL does not include MEXC fees/slippage and can
        # report a fake win while the account balance is down.
        live_cash_before = await self._balance_cash_usdt() if live else None
        if live:
            try:
                # Close the exact exchange row/holdVol when available and keep
                # retrying residual exchange tails until the symbol is flat.
                exchange_row = await self._find_exchange_position_row(symbol)
                if exchange_row:
                    pos["qty"] = self.exchange_position_qty(exchange_row) or qty
                    qty = float(pos.get("qty") or qty or 0)
                    pos["exchange_contracts"] = exchange_row.get("contracts") or (exchange_row.get("info") or {}).get("holdVol")
                    pos["raw_exchange_position"] = exchange_row
                flat, close_attempts = await self._close_until_flat(symbol, reason, side, qty)
                already_closed = flat
                if not flat:
                    raise RuntimeError("exchange position still open after retry close loop")
            except Exception as e:
                err = str(e)
                # MEXC code 2009 means the position is already gone. This is a
                # normal race after TP/SL/time-stop/native-close, not a trading
                # failure, so keep closing idempotent and silent for Telegram.
                if "2009" in err or "nonexistent or closed" in err.lower():
                    already_closed = True
                else:
                    pos["status"] = "open"
                    pos["close_attempts"] = close_attempts
                    await self.storage.upsert_position(pos)
                    return {"ok": False, "reason": f"close failed: {e}", "close_attempts": close_attempts}
        entry = float(pos.get("entry_price") or 0)
        exit_price = float(exit_price or entry)
        pnl_pct = ((exit_price-entry)/entry*100) if str(pos.get("side")).upper()=="LONG" and entry else ((entry-exit_price)/entry*100 if entry else 0)
        # MEXC futures qty can be contracts/holdVol after sync, not base BTC/ETH.
        # Prefer stored USDT notional so tiny 0-fee scalps show real profit instead
        # of 0.0000 when local qty is not base amount.
        try:
            notional = float(pos.get("notional_usdt") or pos.get("planned_notional_usdt") or pos.get("precheck_notional_usdt") or 0)
        except Exception:
            notional = 0.0
        if notional <= 0 and entry > 0:
            notional = abs(float(pos.get("qty") or 0) * entry)
        pnl_usdt = pnl_pct / 100.0 * notional
        pnl_source = "local_price"
        if live and live_cash_before is not None:
            # Let MEXC settle the market close, then use the actual cash delta.
            # This prevents Telegram/session stats from showing a fake win when
            # local mark price is positive but fees/slippage make balance negative.
            try:
                await asyncio.sleep(float(os.getenv("BOOST_REALIZED_PNL_SETTLE_SEC", "2.2")))
            except Exception:
                pass
            live_cash_after = None
            # MEXC can return one stale balance right after a close. Sample a few
            # times and use the last valid cashBalance-like value.
            for _ in range(3):
                live_cash_after = await self._balance_cash_usdt()
                if live_cash_after is not None:
                    try:
                        await asyncio.sleep(0.35)
                    except Exception:
                        pass
            if live_cash_after is not None:
                delta = float(live_cash_after) - float(live_cash_before)
                # Ignore impossible huge deltas caused by a stale/wrong balance read,
                # but accept small negative/positive real execution differences.
                if abs(delta) <= max(10.0, abs(notional) * 0.25 + 2.0):
                    pnl_usdt = delta
                    pnl_source = "exchange_cash_delta_after_fees"
                    if notional > 0:
                        pnl_pct = (pnl_usdt / notional) * 100.0

        await self.storage.add_trade({
            "ts_open": pos.get("opened_at"),
            "ts_close": time.time(),
            "symbol": symbol,
            "side": pos.get("side"),
            "strategy": pos.get("strategy"),
            "mode": "live" if live else "paper",
            "entry_price": entry,
            "exit_price": exit_price,
            "qty": qty,
            "pnl_usdt": pnl_usdt,
            "pnl_pct": pnl_pct,
            "result": "win" if pnl_usdt > 0 else "loss",
            "reason": reason,
            "pnl_source": pnl_source,
            "mirror_used": pos.get("mirror_used", False),
            "session": pos.get("session"),
        })
        # v0078 close confirmation: verify the concrete symbol position after
        # a small grace period. Do not use account/assets hidden margin as a
        # failure signal: MEXC can keep margin fields stale for a few seconds
        # after the position is already closed, which caused duplicate close
        # attempts and noisy 2009 Telegram events.
        confirmed_flat = True
        if live and hasattr(self.exchange_client, "fetch_positions") and not already_closed:
            try:
                await asyncio.sleep(float(os.getenv("POST_CLOSE_POSITION_RECHECK_DELAY_SEC", os.getenv("POST_CLOSE_BALANCE_CHECK_DELAY_SEC", "2.0"))))
                rows = await self.exchange_client.fetch_positions([symbol])
                for r in rows or []:
                    try:
                        if self._position_symbol_matches(r, symbol) and self._exchange_row_contracts(r) > 0:
                            confirmed_flat = False
                            break
                    except Exception:
                        continue
                if confirmed_flat:
                    hidden, hidden_state = await self._mexc_hidden_margin_after_close()
                    if hidden:
                        confirmed_flat = False
                        pos["close_warning"] = "close order sent, but MEXC balance still shows live margin/PnL while open_positions is hidden"
                        pos["hidden_margin_state"] = hidden_state
                if not confirmed_flat:
                    pos["status"] = "open"
                    pos["close_warning"] = pos.get("close_warning") or "close order sent; exchange position still open after grace recheck"
                    pos["updated_at"] = time.time()
                    await self.storage.upsert_position(pos)
            except Exception as e:
                # Verification outages should not create double-close loops. Keep
                # local cleanup and let the next /positions or sync restore state
                # if the exchange still has a real position.
                pos["close_verify_warning"] = str(e)[:220]
        if not confirmed_flat:
            return {"ok": False, "reason": pos.get("close_warning", "close not confirmed"), "pnl_usdt": pnl_usdt, "pnl_pct": pnl_pct}
        await self.storage.remove_position(symbol)
        try:
            cooldown = int(await self.storage.get("cooldown_after_close_sec", 120) or 120)
        except Exception:
            cooldown = int(os.getenv("COOLDOWN_AFTER_CLOSE_SEC", "120") or 120)
        await self.storage.set_lock(symbol, max(0, cooldown), f"closed: {reason}")
        return {"ok": True, "pnl_usdt": pnl_usdt, "pnl_pct": pnl_pct}
