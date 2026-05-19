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
            params = {"clientOrderId": f"bot_entry_{int(time.time()*1000)}"}
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
                    await asyncio.sleep(float(os.getenv("POST_ORDER_POSITION_SYNC_DELAY_SEC", "0.5")))
                    exchange_positions = await self.exchange_client.fetch_positions([plan.symbol])
                    active = [p for p in (exchange_positions or []) if self.exchange_position_qty(p) > 0]
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
            await self.storage.upsert_position(pos)

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
                    auto_close = await self._setting_bool("auto_close_on_protection_failed", "ALLOW_AUTO_CLOSE_ON_PROTECTION_FAILED", False)
                    if auto_close:
                        close_res = await self.close_position(pos, "protection_failed", live=True, exit_price=pos.get("entry_price"))
                        return {"ok": False, "reason": "protection orders failed; auto-close explicitly enabled", "protection": protection, "close_result": close_res}
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

    async def place_protection_orders(self, pos: dict, live: bool) -> dict:
        if not live:
            return {}
        pos = self._sanitize_position_for_exchange(dict(pos))
        symbol = pos["symbol"]
        qty = float(pos.get("qty") or 0)
        if qty <= 0:
            return {}
        side = "sell" if str(pos.get("side")).upper() == "LONG" else "buy"
        out = {"ok": True}
        require_exchange_protection = await self._setting_bool("require_exchange_protection", "REQUIRE_EXCHANGE_PROTECTION", True)
        try:
            tp = float(pos.get("take_price") or 0)
            if tp > 0:
                order = await self._create_take_profit_market_order(symbol, side, qty, tp)
                out["tp_order_id"] = order.get("id")
        except Exception as e:
            out["tp_error"] = str(e)
        try:
            sl = float(pos.get("stop_price") or 0)
            if sl > 0:
                order = await self._create_stop_market_order(symbol, side, qty, sl)
                out["sl_order_id"] = order.get("id")
        except Exception as e:
            out["sl_error"] = str(e)
        if require_exchange_protection and (not out.get("tp_order_id") or not out.get("sl_order_id")):
            out["ok"] = False
        out["protection_status"] = "EXCHANGE PROTECTED" if out.get("tp_order_id") and out.get("sl_order_id") else "LOCAL BOT PROTECTED"
        out["protection_mode"] = "exchange" if out["protection_status"] == "EXCHANGE PROTECTED" else "local_monitoring"
        if out["protection_status"] != "EXCHANGE PROTECTED":
            out["protection_warning"] = "exchange TP/SL not fully confirmed; bot monitors locally"
        else:
            out.pop("protection_warning", None)
        return out

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
                if not confirmed_flat:
                    pos["status"] = "open"
                    pos["close_warning"] = "close order sent; exchange position still open after grace recheck"
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
