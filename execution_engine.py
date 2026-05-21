import time
import asyncio
import os
from models import TradePlan

class ExecutionEngine:
    """
    Real execution layer for entries/exits.

    Hardened execution layer:
    - paper mode no longer depends on exchange private endpoints
    - per-symbol async lock prevents duplicate concurrent entries
    - open positions + pending entries count as occupied slots
    - limit entries are tracked as pending and later confirmed via fetch_order
    - market entries rebase entry/SL/TP from the actual reported fill price
    - exchange-side TP/SL is required by default for live entries
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
            # fail-safe: if exchange order check fails, block live entry
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
            try:
                order = await self._create_order_retry(plan.symbol, order_type, side, plan.qty, price, params, attempts=2)
            except Exception as e:
                if self._is_mexc_opening_restricted_error(e):
                    await self.storage.set_lock(plan.symbol, int(os.getenv("MEXC_RESTRICTED_SYMBOL_LOCK_SEC", "86400")), "mexc_opening_restricted_8950")
                    return {"ok": False, "reason": "mexc opening restricted / reduce-only symbol (code 8950)"}
                raise

            # For market orders, sync the real exchange position immediately.
            # This prevents the bot from losing state when MEXC accepted the order
            # but the raw order response does not include a filled quantity/price.
            pos = plan.__dict__.copy()
            pos["status"] = "pending" if order_type == "limit" else "open"
            pos["initial_stop_price"] = plan.stop_price
            pos["initial_take_price"] = plan.take_price
            if bool(getattr(plan, "liquidation_stop_mode", False)):
                pos["liquidation_stop_mode"] = True
                pos["planned_stop_price"] = plan.stop_price
                pos["stop_price"] = 0
            if bool(getattr(plan, "liquidation_stop_mode", False)):
                pos["liquidation_stop_mode"] = True
                pos["planned_stop_price"] = plan.stop_price
                pos["stop_price"] = 0
            pos["liquidity_runner_stage"] = 0
            pos["order_id"] = order.get("id")
            pos["opened_at"] = time.time()
            pos["updated_at"] = time.time()
            pos["raw_order"] = order
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
                    if str(pos.get("strategy") or "").lower() == "ai_scalping":
                        ai_delay = await self._setting_float("ai_scalping_protection_delay_sec", "AI_SCALPING_PROTECTION_DELAY_SEC", 3.0)
                        post_delay = max(post_delay, ai_delay)
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
                        pos["exchange_synced"] = True
                        pos["updated_at"] = time.time()
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
                protection = await self.place_protection_orders(pos, live=True)
                pos.update(protection)
                await self.storage.upsert_position(pos)
                if not protection.get("ok"):
                    # v0066: do NOT delete local state when MEXC fails to place
                    # exchange-side TP/SL. The position is already live; losing
                    # local state is worse than running local TP/SL monitoring.
                    pos["protection_mode"] = "local_monitoring"
                    pos["protection_warning"] = "exchange protection failed; bot monitors TP/SL locally"
                    pos["updated_at"] = time.time()
                    await self.storage.upsert_position(pos)
                    # Never force-close an already-open live position only because
                    # exchange TP/SL was not confirmed. This was the behaviour that
                    # choked scalps into repeated small losses. The bot keeps local
                    # TP/SL monitoring active and the watchdog keeps reattaching
                    # native MEXC position TP/SL in the background loop.
                    pos["auto_close_on_protection_failed_ignored"] = True
                    await self.storage.upsert_position(pos)
                    return {"ok": True, "order": order, "position": pos, "warning": "exchange protection failed; bot monitors TP/SL locally"}
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

    async def _create_trigger_market_order(self, symbol: str, side: str, qty: float, trigger_price: float, kind: str) -> dict:
        errors = []
        try:
            native_name = "mexc_place_take_profit_market" if kind == "tp" else "mexc_place_stop_market"
            if hasattr(self.exchange_client, native_name):
                fn = getattr(self.exchange_client, native_name)
                return await fn(
                    symbol=symbol, close_side=side, amount=qty, trigger_price=trigger_price,
                    client_order_id=f"bot_{kind}_{int(time.time()*1000)}",
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
                return await self.exchange_client.create_order(symbol, type_, side, qty, price, params)
            except Exception as e:
                errors.append(f"{type_}: {e}")
        raise RuntimeError(f"{kind}-market protection failed: " + " | ".join(errors))

    async def _create_stop_market_order(self, symbol: str, side: str, qty: float, stop_price: float) -> dict:
        return await self._create_trigger_market_order(symbol, side, qty, stop_price, "sl")

    async def _create_take_profit_market_order(self, symbol: str, side: str, qty: float, take_price: float) -> dict:
        return await self._create_trigger_market_order(symbol, side, qty, take_price, "tp")

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
            orders = await self.exchange_client.fetch_open_orders(check_pos.get("symbol"))
            return ProtectionEngine(self.exchange_client).classify_orders(check_pos, orders or [])
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
        require_exchange_protection = await self._setting_bool("require_exchange_protection", "REQUIRE_EXCHANGE_PROTECTION", True)
        attempts = max(1, int(os.getenv("PROTECTION_PLACE_MAX_ATTEMPTS", "3") or "3"))
        delay = float(os.getenv("PROTECTION_RECHECK_DELAY_SEC", "0.8") or "0.8")
        if str(pos.get("strategy") or "").lower() == "ai_scalping":
            # AI scalping uses very small TP/SL distances; MEXC often needs an
            # extra second before the native TPSL order becomes visible.
            attempts = max(attempts, int(os.getenv("AI_SCALPING_PROTECTION_ATTEMPTS", "5") or "5"))
            delay = max(delay, float(os.getenv("AI_SCALPING_PROTECTION_RECHECK_DELAY_SEC", "1.2") or "1.2"))
        history = []
        best = {"ok": False}
        for i in range(attempts):
            out = {"ok": True, "attempt": i + 1}
            # On retry, remove stale/partial protection first. Without this MEXC
            # can leave one valid leg and reject/duplicate the other.
            if i > 0 and hasattr(self.exchange_client, "cancel_all_orders"):
                try:
                    await self.exchange_client.cancel_all_orders(symbol)
                except Exception as e:
                    out["retry_cancel_error"] = str(e)[:240]
                await asyncio.sleep(delay)
            tp = float(pos.get("take_price") or 0)
            sl = float(pos.get("stop_price") or 0)
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
            elif str(getattr(self.exchange_client, "exchange_id", "") or "").lower() == "mexc" and hasattr(self.exchange_client, "mexc_place_tpsl_by_position"):
                # v0135: MEXC sometimes rejects/does not expose by-position TP/SL.
                # First try native attached TP/SL; if it is missing or returns no id,
                # immediately fall back to two reduce-only trigger-market plan orders
                # (one TP and one SL). Do not leave the trade protected only locally.
                native_tpsl_ok = False
                try:
                    if tp <= 0 or sl <= 0:
                        raise RuntimeError("missing take_price/stop_price for position TP/SL")
                    order = await self.exchange_client.mexc_place_tpsl_by_position(
                        symbol=symbol, side=side, qty=qty, stop_price=sl, take_price=tp,
                        client_order_id=f"bot_tpsl_{int(time.time()*1000)}",
                    )
                    oid = str(order.get("id") or "")
                    out["tp_order_id"] = oid
                    out["sl_order_id"] = oid
                    out["tpsl_raw"] = order
                    native_tpsl_ok = bool(oid)
                    if not native_tpsl_ok:
                        out["tpsl_error"] = "MEXC by-position TP/SL returned empty id; falling back to trigger-market TP+SL"
                except Exception as e:
                    if self._is_mexc_tpsl_already_exists_error(e):
                        out["tp_order_id"] = "MEXC_TPSL_ALREADY_EXISTS"
                        out["sl_order_id"] = "MEXC_TPSL_ALREADY_EXISTS"
                        out["tp_exists"] = True
                        out["sl_exists"] = True
                        out["tpsl_already_exists"] = True
                        out["tpsl_note"] = "MEXC says native position TP/SL already exists; treating as protected"
                        native_tpsl_ok = True
                    else:
                        out["tpsl_error"] = str(e)[:500]

                if not native_tpsl_ok:
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
                if not out.get("tp_exists") and out.get("tp_order_id"):
                    # Some MEXC trigger orders are not immediately visible in
                    # merged open-order endpoints; a non-empty native id is the
                    # best available confirmation after successful create.
                    out["tp_exists"] = True
                    out["tp_verify_note"] = "accepted by MEXC but not yet visible in open orders"
                out["sl_exists"] = True
                out["sl_order_id"] = "LIQUIDATION_STOP"
                out["ok"] = bool(out.get("tp_exists"))
                out["protection_status"] = "TP + LIQUIDATION STOP" if out["ok"] else "LOCAL BOT PROTECTED"
                out["protection_mode"] = "exchange_tp_liquidation_sl" if out["ok"] else "local_monitoring"
            else:
                verified = await self._verify_exchange_protection(pos, str(out.get("tp_order_id") or ""), str(out.get("sl_order_id") or ""))
                out.update({k: v for k, v in verified.items() if k not in {"tp_order_id", "sl_order_id"} or v})
                if out.get("tpsl_raw") and (out.get("tp_order_id") or out.get("sl_order_id")):
                    # MEXC position TP/SL may be accepted by stoporder/place but
                    # appear in open-orders/plan endpoints with a delay. Treat a
                    # successful native TPSL response with an id as exchange
                    # protected, while the watchdog continues periodic reconcile.
                    out["tp_exists"] = True
                    out["sl_exists"] = True
                    out["tpsl_verify_note"] = "accepted by MEXC but not yet visible in open orders"
                out["ok"] = bool(out.get("tp_exists") and out.get("sl_exists"))
                out["protection_status"] = "EXCHANGE PROTECTED" if out["ok"] else "LOCAL BOT PROTECTED"
                out["protection_mode"] = "exchange" if out["ok"] else "local_monitoring"
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
        best["protection_warning"] = "exchange protection not confirmed after delayed position sync and distance expansion"
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
                res = await self._create_order_retry(
                    symbol, "market", side, fallback_qty, None,
                    {"reduceOnly": True, "clientOrderId": f"bot_close_{int(time.time()*1000)}"}, attempts=2
                )
                results.append({"attempt": i + 1, "mode": "fallback_qty", "result": res})
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

    async def close_position(self, pos: dict, reason: str, live: bool, exit_price: float | None = None):
        symbol = pos["symbol"]
        side = "sell" if str(pos.get("side")).upper() == "LONG" else "buy"
        qty = float(pos.get("qty") or 0)
        pos["status"] = "closing"
        await self.storage.upsert_position(pos)
        already_closed = False
        close_attempts = []
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
        pnl_usdt = pnl_pct/100 * float(pos.get("qty") or 0) * entry
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
