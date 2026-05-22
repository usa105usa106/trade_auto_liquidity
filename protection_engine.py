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
                local_pid = str(pos.get("position_id") or pos.get("positionId") or pos.get("id") or "")
                row_pid = str(info.get("positionId") or info.get("position_id") or "")
                pid_ok = (not local_pid) or (not row_pid) or (local_pid == row_pid)
                if pid_ok and raw_tp > 0:
                    found_tp = True; matched_tp_id = oid or row_pid or matched_tp_id
                if pid_ok and raw_sl > 0:
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
            orders = await self.exchange_client.fetch_open_orders(symbol)
            return self.classify_orders(pos, orders or [])
        except Exception as e:
            return {"protection_status": "LOCAL BOT PROTECTED", "protection_mode": "local_monitoring", "protection_error": str(e)[:240], "tp_exists": False, "sl_exists": False}

    async def reconcile(self, pos: dict, live: bool = True, reattach: bool = True) -> dict:
        out = await self.check(pos)
        if out.get("protection_status") in {"EXCHANGE PROTECTED", "TP + LIQUIDATION STOP"} or not reattach or not live or not self.execution_engine:
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
        # protection set atomically: cancel old TP/SL/plan orders first, then
        # recreate both legs with the current qty/SL/TP. This prevents duplicate
        # stale stops and makes breakeven/runner SL updates effective on MEXC.
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
