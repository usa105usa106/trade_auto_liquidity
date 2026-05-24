from __future__ import annotations

import time
from typing import Any


class ProtectionEngine:
    """Reconcile exchange TP/SL protection for a local position.

    MEXC can expose normal reduce-only TP orders and trigger/plan/stop orders
    through different endpoints. This helper classifies open orders instead of
    treating any order as protection.
    """

    def __init__(self, exchange_client, execution_engine=None):
        self.exchange_client = exchange_client
        self.execution_engine = execution_engine

    def _norm_id(self, value: Any) -> str:
        return str(value or "").strip()

    def _is_boost_emergency_only(self, pos: dict) -> bool:
        """Detect BOOST/HUNTER emergency-SL-only positions robustly.

        Old watchdog paths sometimes downgraded protection_mode to local_fast
        after a false miss, so relying only on protection_mode caused the next
        reconcile pass to call cancel_all.  A HUNTER position is emergency-only
        if it is boost_scalping OR carries the live-trailing TP marker / boost
        flags / bot_sl planorder id.
        """
        strategy = str(pos.get("strategy") or "").lower()
        mode = str(pos.get("protection_mode") or "").lower()
        tp_id = str(pos.get("tp_order_id") or "")
        sl_id = str(pos.get("sl_order_id") or "")
        return (
            strategy == "boost_scalping"
            or mode in {"exchange_emergency_sl_only", "unsafe_no_emergency_sl", "local_fast"}
            or tp_id == "LIVE_TRAILING_NO_FIXED_TP"
            or bool(pos.get("boost_emergency_sl_only"))
            or bool(pos.get("boost_unsafe_position"))
            or (sl_id and str(pos.get("take_price") or "") and strategy in {"", "boost_scalping"})
        )

    def _order_text(self, order: dict) -> str:
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        parts = [
            order.get("id"), order.get("type"), order.get("side"), order.get("clientOrderId"),
            info.get("externalOid"), info.get("orderType"), info.get("type"), info.get("category"),
            info.get("_source_endpoint"), info.get("_protection_kind"), info.get("triggerPrice"), info.get("stopPrice"), info.get("takeProfitPrice"), info.get("stopLossPrice"),
        ]
        return " ".join(str(p).lower() for p in parts if p not in (None, ""))

    def _is_close_order(self, order: dict) -> bool:
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        txt = self._order_text(order)
        if any(k in txt for k in ("reduce", "close", "bot_tp", "bot_sl", "tpsl", "stop", "planorder")):
            return True
        for key in ("reduceOnly", "reduce_only", "closeOrder", "closePosition"):
            if str(info.get(key)).lower() in {"1", "true", "yes"}:
                return True
        # MEXC close side codes: 2 closes short, 4 closes long.
        if str(info.get("side") or order.get("side")) in {"2", "4"}:
            return True
        return False

    def classify_orders(self, pos: dict, orders: list[dict]) -> dict:
        side = str(pos.get("side") or "").upper()
        close_side = "sell" if side == "LONG" else "buy"
        tp_id = self._norm_id(pos.get("tp_order_id"))
        sl_id = self._norm_id(pos.get("sl_order_id"))
        found_tp = False
        found_sl = False
        matched_tp_id = ""
        matched_sl_id = ""
        tp_price = float(pos.get("take_price") or 0)
        sl_price = float(pos.get("stop_price") or 0)

        for order in orders or []:
            if not self._is_close_order(order):
                continue
            oid = self._norm_id(order.get("id"))
            side_ok = str(order.get("side") or "").lower() in {"", close_side}
            txt = self._order_text(order)
            info = order.get("info") if isinstance(order.get("info"), dict) else {}
            price = 0.0
            for key in ("price", "triggerPrice", "stopPrice", "executePrice", "_protection_price", "takeProfitPrice", "stopLossPrice"):
                try:
                    raw = order.get(key) if key in order else info.get(key)
                    if raw not in (None, ""):
                        price = float(raw)
                        break
                except Exception:
                    pass
            kind = str(info.get("_protection_kind") or "").lower()

            # v0164: MEXC native /stoporder/place exposes one active row for a
            # position with BOTH takeProfitPrice and stopLossPrice.  In that row
            # vol/realityVol can be 0 because volType=2 means "same as position".
            # Treat such active native rows as valid exchange protection even
            # when pseudo-order ids or parsed amount are not normal order-like.
            src = str(info.get("_source_endpoint") or "").lower()
            is_active_native_tpsl = (
                "stoporder" in src
                and str(info.get("state", 1)) in {"1", ""}
                and str(info.get("isFinished", info.get("is_finished", 0))).lower() in {"0", "false", ""}
                and str(info.get("errorCode", 0)) in {"0", ""}
            )
            if is_active_native_tpsl:
                try:
                    raw_tp = float(info.get("takeProfitPrice") or 0)
                except Exception:
                    raw_tp = 0.0
                try:
                    raw_sl = float(info.get("stopLossPrice") or 0)
                except Exception:
                    raw_sl = 0.0
                # v0165: MEXC native /stoporder/place may return a combined row
                # with vol=0 because volType=2 means SAME/full position.  If the
                # row is active and has non-zero TP/SL prices, it is valid
                # exchange protection.  Do not reject it just because local
                # planned prices differ slightly or the row was expanded into
                # pseudo-orders.  Prefer a positionId match when available, but
                # fall back to symbol-level matching because MEXC sometimes
                # stores ids as strings or local rows may not have positionId.
                # v0166: do NOT use generic pos["id"] for position matching.
                # In this bot it can be a local/order id and will not match MEXC
                # positionId, causing false LOCAL PROTECTION warnings even when
                # /stoporder/open_orders shows active native TP/SL.  Only compare
                # explicit exchange position ids; otherwise accept symbol/side +
                # active native row with valid TP/SL prices.
                raw_pos = pos.get("raw_exchange_position") if isinstance(pos.get("raw_exchange_position"), dict) else {}
                raw_info = raw_pos.get("info") if isinstance(raw_pos.get("info"), dict) else {}
                local_pid = str(
                    pos.get("position_id")
                    or pos.get("positionId")
                    or pos.get("exchange_position_id")
                    or raw_pos.get("positionId")
                    or raw_pos.get("id")
                    or raw_info.get("positionId")
                    or raw_info.get("position_id")
                    or ""
                ).strip()
                row_pid = str(info.get("positionId") or info.get("position_id") or "").strip()
                pid_ok = (not local_pid) or (not row_pid) or (local_pid == row_pid)
                # If position ids differ but the native row is active and prices
                # match the planned protection closely, still accept it.  This
                # covers locally restored positions where local ids lag MEXC.
                price_ok = True
                if tp_price > 0 and raw_tp > 0:
                    price_ok = price_ok and (abs(raw_tp - tp_price) / tp_price < 0.003)
                if sl_price > 0 and raw_sl > 0:
                    price_ok = price_ok and (abs(raw_sl - sl_price) / sl_price < 0.003)
                native_row_ok = pid_ok or price_ok
                if native_row_ok and raw_tp > 0:
                    found_tp = True; matched_tp_id = oid or row_pid or matched_tp_id
                if native_row_ok and raw_sl > 0:
                    found_sl = True; matched_sl_id = oid or row_pid or matched_sl_id

            if side_ok and (kind == "tp" or oid and oid == tp_id or "bot_tp" in txt or "take" in txt or "tp" in txt or "/tpsl/" in txt):
                found_tp = True; matched_tp_id = oid or matched_tp_id
            if side_ok and (kind == "sl" or oid and oid == sl_id or "bot_sl" in txt or "stop" in txt or "sl" in txt or "planorder" in txt or "stoporder" in txt):
                found_sl = True; matched_sl_id = oid or matched_sl_id
            # Fallback by trigger/limit price when client ids are unavailable.
            if side_ok and tp_price > 0 and price > 0 and abs(price - tp_price) / tp_price < 0.002:
                found_tp = True; matched_tp_id = oid or matched_tp_id
            if side_ok and sl_price > 0 and price > 0 and abs(price - sl_price) / sl_price < 0.002:
                found_sl = True; matched_sl_id = oid or matched_sl_id

        liq_mode = bool(pos.get("liquidation_stop_mode")) and str(pos.get("strategy") or "").lower() == "ai_scalping"
        if liq_mode:
            # In AI liquidation-stop mode no exchange SL should exist: liquidation
            # is the planned hard stop.  Only TP must be present/confirmed.
            status = "TP + LIQUIDATION STOP" if found_tp else "LOCAL BOT PROTECTED"
            return {
                "tp_exists": found_tp,
                "sl_exists": True,
                "take_profit_ok": found_tp,
                "stop_loss_ok": True,
                "tp_order_id": matched_tp_id or pos.get("tp_order_id"),
                "sl_order_id": "LIQUIDATION_STOP",
                "protection_status": status,
                "protection_mode": "exchange_tp_liquidation_sl" if found_tp else "local_monitoring",
                "checked_at": time.time(),
            }

        # HUNTER/BOOST uses live trailing/momentum exits and places ONLY an
        # emergency exchange SL.  The SL is often created as a MEXC planorder,
        # not as stoporder/open_orders.  Older code required both TP and SL here;
        # then it saw stoporder=[] after a successful planorder/place and marked
        # the position unsafe, sometimes canceling the real planorder.  For BOOST,
        # an active SL plan/stop order is enough exchange protection.
        is_boost = str(pos.get("strategy") or "").lower() == "boost_scalping"
        emergency_only = self._is_boost_emergency_only(pos)
        if emergency_only:
            status = "EMERGENCY SL ONLY" if found_sl else "UNSAFE POSITION"
            return {
                "tp_exists": True,
                "sl_exists": found_sl,
                "take_profit_ok": True,
                "stop_loss_ok": found_sl,
                "tp_order_id": pos.get("tp_order_id") or "LIVE_TRAILING_NO_FIXED_TP",
                "sl_order_id": matched_sl_id or pos.get("sl_order_id"),
                "protection_status": status,
                "protection_mode": "exchange_emergency_sl_only" if found_sl else "unsafe_no_emergency_sl",
                "boost_unsafe_position": not found_sl,
                "boost_defensive_mode": not found_sl,
                "checked_at": time.time(),
            }

        status = "EXCHANGE PROTECTED" if (found_tp and found_sl) else "LOCAL BOT PROTECTED"
        return {
            "tp_exists": found_tp,
            "sl_exists": found_sl,
            "take_profit_ok": found_tp,
            "stop_loss_ok": found_sl,
            "tp_order_id": matched_tp_id or pos.get("tp_order_id"),
            "sl_order_id": matched_sl_id or pos.get("sl_order_id"),
            "protection_status": status,
            "protection_mode": "exchange" if status == "EXCHANGE PROTECTED" else "local_monitoring",
            "checked_at": time.time(),
        }

    async def check(self, pos: dict) -> dict:
        symbol = pos.get("symbol")
        if not symbol:
            return {"protection_status": "LOCAL BOT PROTECTED", "protection_mode": "local_monitoring", "protection_error": "missing symbol", "tp_exists": False, "sl_exists": False}
        try:
            # BOOST/HUNTER emergency SL is often a MEXC planorder.  Verify that
            # exact planorder first, otherwise fetch_open_orders may surface only
            # stoporder=[] in logs and the watchdog falsely marks the position
            # UNSAFE, then cancels/retries protection in a loop.
            is_boost = str(pos.get("strategy") or "").lower() == "boost_scalping"
            emergency_only = self._is_boost_emergency_only(pos)
            sl_id = str(pos.get("sl_order_id") or "").strip()
            # BOOST/HUNTER emergency SL is normally a planorder.  Do not fall
            # through to generic fetch_open_orders()/stoporder/* when sl_id is
            # missing/stale: search planorder by id first, then by active bot_sl
            # plan for this symbol.  This prevents the watchdog from spamming
            # stoporder/* and cancel_all while a valid planorder exists.
            if emergency_only and hasattr(self.exchange_client, "mexc_find_active_plan_order"):
                row = await self.exchange_client.mexc_find_active_plan_order(symbol, order_id=sl_id)
                if not row:
                    row = await self.exchange_client.mexc_find_active_plan_order(symbol)
                if row:
                    try:
                        from debug_log import log_event
                        log_event("boost_planorder_emergency_sl_verified", symbol=symbol, sl_order_id=sl_id, endpoint="planorder/list/orders", ok=True)
                    except Exception:
                        pass
                    return {
                        "tp_exists": True,
                        "sl_exists": True,
                        "take_profit_ok": True,
                        "stop_loss_ok": True,
                        "tp_order_id": pos.get("tp_order_id") or "LIVE_TRAILING_NO_FIXED_TP",
                        "sl_order_id": sl_id,
                        "protection_status": "EMERGENCY SL ONLY",
                        "protection_mode": "exchange_emergency_sl_only",
                        "boost_unsafe_position": False,
                        "boost_defensive_mode": False,
                        "checked_at": time.time(),
                    }
            if emergency_only:
                # In BOOST emergency-SL-only mode, absence of a planorder means
                # UNSAFE; do NOT query stoporder/* here.  Reconcile will retry
                # placing the plan SL without destructive cancel_all.
                return {
                    "tp_exists": True,
                    "sl_exists": False,
                    "take_profit_ok": True,
                    "stop_loss_ok": False,
                    "tp_order_id": pos.get("tp_order_id") or "LIVE_TRAILING_NO_FIXED_TP",
                    "sl_order_id": sl_id,
                    "protection_status": "UNSAFE POSITION",
                    "protection_mode": "unsafe_no_emergency_sl",
                    "boost_unsafe_position": True,
                    "boost_defensive_mode": True,
                    "checked_at": time.time(),
                }

            # v0248: Quick Bounce uses MEXC /planorder/place; Impulse Dump uses
            # MEXC native /stoporder/place attached to a position.  Both can lag
            # in list/open-orders endpoints right after placement.  If the entry
            # code already posted native TP/SL successfully, keep the position
            # EXCHANGE PROTECTED during a short indexing grace period instead of
            # showing a false LOCAL PROTECTION/MISSING warning or trying to
            # restore/cancel orders.
            strategy_l = str(pos.get("strategy") or "").lower()
            is_impulse_dump = strategy_l == "impulse_dump"
            if is_impulse_dump and (pos.get("tpsl_native_direct_posted") or pos.get("tpsl_native_direct_raw")):
                try:
                    opened_at = float(pos.get("opened_at") or pos.get("created_at") or 0)
                except Exception:
                    opened_at = 0.0
                tp_price = float(pos.get("take_price") or 0)
                sl_price = float(pos.get("stop_price") or 0)
                raw = pos.get("tpsl_native_direct_raw") if isinstance(pos.get("tpsl_native_direct_raw"), dict) else {}
                native_id = str(raw.get("id") or raw.get("data") or pos.get("tp_order_id") or "MEXC_NATIVE_TPSL")
                grace = 180.0
                if tp_price > 0 and sl_price > 0 and (not opened_at or time.time() - opened_at < grace):
                    return {
                        "tp_exists": True,
                        "sl_exists": True,
                        "take_profit_ok": True,
                        "stop_loss_ok": True,
                        "tp_order_id": pos.get("tp_order_id") or native_id,
                        "sl_order_id": pos.get("sl_order_id") or native_id,
                        "protection_status": "EXCHANGE PROTECTED",
                        "protection_mode": "exchange_native_tpsl_pending_verify",
                        "protection_note": "MEXC native stoporder TP/SL accepted; open_orders verification in grace period",
                        "checked_at": time.time(),
                    }

            # v0233 Quick Bounce uses MEXC /planorder/place for standalone
            # reduce-only TP/SL. Those orders do NOT appear in
            # /stoporder/open_orders, so checking generic open orders can produce
            # false LOCAL PROTECTION messages right after a successful placement.
            # Verify the exact saved planorder ids first and trust them while
            # active. This also prevents watchdog reattach/cancel loops and
            # duplicate TP/SL orders.
            is_quick_bounce = strategy_l == "quick_bounce"
            if is_quick_bounce and hasattr(self.exchange_client, "mexc_find_active_plan_order"):
                tp_id = str(pos.get("tp_order_id") or "").strip()
                sl_id_qb = str(pos.get("sl_order_id") or "").strip()
                tp_row = await self.exchange_client.mexc_find_active_plan_order(symbol, order_id=tp_id) if tp_id else {}
                sl_row = await self.exchange_client.mexc_find_active_plan_order(symbol, order_id=sl_id_qb) if sl_id_qb else {}
                if tp_row and sl_row:
                    return {
                        "tp_exists": True,
                        "sl_exists": True,
                        "take_profit_ok": True,
                        "stop_loss_ok": True,
                        "tp_order_id": tp_id,
                        "sl_order_id": sl_id_qb,
                        "protection_status": "EXCHANGE PROTECTED",
                        "protection_mode": "exchange_planorder",
                        "protection_note": "MEXC planorder TP/SL verified by id",
                        "checked_at": time.time(),
                    }
                # If the position was just created and MEXC list endpoint has not
                # indexed planorders yet, do not immediately downgrade. The
                # successful /planorder/place ids are strong enough to avoid a
                # false warning during the grace period.
                try:
                    opened_at = float(pos.get("opened_at") or pos.get("created_at") or 0)
                except Exception:
                    opened_at = 0.0
                grace = 90.0
                if tp_id and sl_id_qb and opened_at and time.time() - opened_at < grace:
                    return {
                        "tp_exists": True,
                        "sl_exists": True,
                        "take_profit_ok": True,
                        "stop_loss_ok": True,
                        "tp_order_id": tp_id,
                        "sl_order_id": sl_id_qb,
                        "protection_status": "EXCHANGE PROTECTED",
                        "protection_mode": "exchange_planorder_pending_verify",
                        "protection_note": "MEXC planorder ids accepted; list verification in grace period",
                        "checked_at": time.time(),
                    }

            orders = await self.exchange_client.fetch_open_orders(symbol)
            return self.classify_orders(pos, orders or [])
        except Exception as e:
            return {"protection_status": "LOCAL BOT PROTECTED", "protection_mode": "local_monitoring", "protection_error": str(e)[:240], "tp_exists": False, "sl_exists": False}

    async def reconcile(self, pos: dict, live: bool = True, reattach: bool = True) -> dict:
        out = await self.check(pos)
        if out.get("protection_status") in {"EXCHANGE PROTECTED", "TP + LIQUIDATION STOP", "EMERGENCY SL ONLY"} or not reattach or not live or not self.execution_engine:
            return out
        # v0233: Quick Bounce fallback is intentionally virtual monitoring.
        # If exchange TP/SL are not confirmed, keep the position open and do
        # NOT cancel/recreate planorders from the watchdog; that can duplicate
        # orders or remove a valid backstop while MEXC list endpoints lag.
        if str(pos.get("strategy") or "").lower() in {"quick_bounce", "impulse_dump"}:
            out["protection_status"] = "VIRTUAL_PROTECTED"
            out["protection_mode"] = "virtual"
            out["virtual_tp_sl_active"] = True
            out["quick_bounce_no_reattach"] = True
            out["impulse_dump_no_reattach"] = str(pos.get("strategy") or "").lower() == "impulse_dump"
            out["protection_note"] = "Quick Bounce/Impulse Dump use local TP/SL fallback; watchdog does not reattach/cancel exchange orders"
            return out
        liq_mode = bool(pos.get("liquidation_stop_mode")) and str(pos.get("strategy") or "").lower() == "ai_scalping"
        try:
            if float(pos.get("qty") or 0) <= 0 or float(pos.get("take_price") or 0) <= 0 or ((not liq_mode) and float(pos.get("stop_price") or 0) <= 0):
                out["reattach_error"] = "missing qty/TP" if liq_mode else "missing qty/SL/TP"
                return out
        except Exception:
            out["reattach_error"] = "invalid qty/TP" if liq_mode else "invalid qty/SL/TP"
            return out
        # If one leg is missing or stale after restart, replace the symbol's
        # protection set atomically.  Exception: BOOST emergency-SL-only mode.
        # There a valid SL may be a planorder, and destructive cancel_all can
        # remove the only real emergency backstop.  Retry placing SL without
        # cancel_all unless the user explicitly runs Cancel All/Panic.
        is_boost = str(pos.get("strategy") or "").lower() == "boost_scalping"
        emergency_only = self._is_boost_emergency_only(pos)
        if is_boost or emergency_only:
            # BOOST/HUNTER must never run cancel_all from watchdog.  It can
            # delete the only emergency planorder backstop while the bot is live.
            out["cancel_before_reattach_skipped"] = "BOOST/HUNTER watchdog: never cancel possible emergency planorder backstop"
        else:
            try:
                if hasattr(self.exchange_client, "cancel_all_orders"):
                    await self.exchange_client.cancel_all_orders(pos.get("symbol"))
            except Exception as e:
                out["cancel_before_reattach_error"] = str(e)[:240]
        prot = await self.execution_engine.place_protection_orders(pos, live=True)
        out.update({"reattach_attempted": True, **prot})
        # Re-check after placement to avoid trusting a partially failed create_order response.
        after = await self.check({**pos, **out})
        out.update(after)
        return out
