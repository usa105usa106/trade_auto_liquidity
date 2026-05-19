from __future__ import annotations

import os
import time
from typing import Any


class RecoveryEngine:
    """Restore bot state from real MEXC positions after restart.

    This is intentionally exchange-first: MEXC positions are the source of truth.
    Local SQLite rows are used only to recover original TP/SL when available.
    """

    def __init__(self, storage, exchange_client, execution_engine):
        self.storage = storage
        self.exchange_client = exchange_client
        self.execution_engine = execution_engine

    def _norm(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value).upper().strip()
        text = text.replace("/USDT:USDT", "_USDT")
        text = text.replace("/", "_").replace("-", "_")
        if text.endswith("USDT") and "_" not in text:
            text = text[:-4] + "_USDT"
        return text

    def _keys(self, pos: dict) -> set[str]:
        keys = set()
        for key in ("symbol", "mexc_symbol"):
            if pos.get(key):
                keys.add(self._norm(pos.get(key)))
        for v in pos.get("symbol_variants") or []:
            keys.add(self._norm(v))
        info = pos.get("info") or {}
        if isinstance(info, dict):
            for key in ("symbol", "contract"):
                if info.get(key):
                    keys.add(self._norm(info.get(key)))
        return {k for k in keys if k}

    def _side(self, ex_pos: dict) -> str:
        side = str(ex_pos.get("side") or "").upper()
        info = ex_pos.get("info") or {}
        raw = str(info.get("positionType") or info.get("holdSide") or info.get("side") or "").lower() if isinstance(info, dict) else ""
        if "SHORT" in side or raw in {"2", "short", "sell"}:
            return "SHORT"
        return "LONG"

    def _entry(self, ex_pos: dict) -> float:
        info = ex_pos.get("info") or {}
        for key in ("entryPrice", "entry_price", "openAvgPrice", "holdAvgPrice"):
            try:
                val = ex_pos.get(key)
                if val not in (None, "") and float(val) > 0:
                    return float(val)
            except Exception:
                pass
            try:
                if isinstance(info, dict):
                    val = info.get(key)
                    if val not in (None, "") and float(val) > 0:
                        return float(val)
            except Exception:
                pass
        return 0.0

    def _qty(self, ex_pos: dict) -> float:
        try:
            return float(self.execution_engine.exchange_position_qty(ex_pos) or 0)
        except Exception:
            return 0.0

    def _fallback_tp_sl(self, ex_pos: dict) -> tuple[float, float]:
        """Conservative emergency TP/SL for exchange-only recovered rows.

        Used only when original local plan was lost after restart/redeploy.
        Defaults mirror the planner's conservative bounds and can be overridden.
        """
        entry = self._entry(ex_pos)
        if entry <= 0:
            return 0.0, 0.0
        side = self._side(ex_pos)
        sl_pct = float(os.getenv("RECOVERY_LOCAL_SL_PCT", os.getenv("MAX_SL_PCT", "0.60")) or 0.60) / 100.0
        tp_pct = float(os.getenv("RECOVERY_LOCAL_TP_PCT", os.getenv("MIN_TP_PCT", "0.25")) or 0.25) / 100.0
        if side == "SHORT":
            return entry * (1 + sl_pct), entry * (1 - tp_pct)
        return entry * (1 - sl_pct), entry * (1 + tp_pct)

    async def _protection_exists(self, symbol: str) -> bool:
        try:
            orders = await self.exchange_client.fetch_open_orders(symbol)
            return bool(orders)
        except Exception:
            return False

    async def recover(self, reattach: bool = True) -> dict:
        report = {
            "exchange_positions": 0,
            "local_before": 0,
            "restored": 0,
            "updated": 0,
            "protection_ok": 0,
            "local_monitoring": 0,
            "reattach_attempted": 0,
            "reattach_ok": 0,
            "errors": [],
        }
        try:
            exchange_positions = [p for p in (await self.exchange_client.fetch_positions() or []) if self._qty(p) > 0]
        except Exception as e:
            report["errors"].append(f"fetch_positions: {e}")
            return report
        local_positions = await self.storage.positions()
        report["exchange_positions"] = len(exchange_positions)
        report["local_before"] = len(local_positions)
        local_by_key: dict[str, dict] = {}
        for lp in local_positions:
            for k in self._keys(lp):
                local_by_key[k] = lp

        for ep in exchange_positions:
            keys = self._keys(ep)
            old = next((local_by_key[k] for k in keys if k in local_by_key), None)
            symbol = ep.get("symbol") or (old or {}).get("symbol")
            if not symbol:
                report["errors"].append("position without symbol")
                continue
            entry = self._entry(ep)
            qty = self._qty(ep)
            side = self._side(ep)
            stop_price = (old or {}).get("stop_price")
            take_price = (old or {}).get("take_price")
            if not stop_price or not take_price:
                stop_price, take_price = self._fallback_tp_sl(ep)
            pos = dict(old or {})
            pos.update({
                "symbol": symbol,
                "mexc_symbol": ep.get("mexc_symbol") or (ep.get("info") or {}).get("symbol"),
                "symbol_variants": sorted(keys),
                "side": side,
                "status": "open",
                "entry_price": entry or pos.get("entry_price"),
                "qty": qty or pos.get("qty"),
                "stop_price": float(stop_price or 0),
                "take_price": float(take_price or 0),
                "strategy": pos.get("strategy") or "recovered_exchange",
                "opened_at": pos.get("opened_at") or time.time(),
                "updated_at": time.time(),
                "exchange_recovered": True,
                "raw_exchange_position": ep,
            })
            has_protection = await self._protection_exists(symbol)
            if has_protection:
                pos["protection_mode"] = "exchange"
                pos.pop("protection_warning", None)
                report["protection_ok"] += 1
            else:
                pos["protection_mode"] = "local_monitoring"
                pos["protection_warning"] = "recovered after restart; exchange TP/SL not found; local TP/SL monitor active"
                report["local_monitoring"] += 1
                if reattach and float(pos.get("qty") or 0) > 0 and float(pos.get("stop_price") or 0) > 0 and float(pos.get("take_price") or 0) > 0:
                    report["reattach_attempted"] += 1
                    try:
                        prot = await self.execution_engine.place_protection_orders(pos, live=True)
                        pos.update(prot)
                        if prot.get("ok"):
                            pos["protection_mode"] = "exchange"
                            pos.pop("protection_warning", None)
                            report["reattach_ok"] += 1
                    except Exception as e:
                        pos["reattach_error"] = str(e)[:240]
                        report["errors"].append(f"reattach {symbol}: {e}")
            pos = self.execution_engine._decorate_position_metrics(pos)
            await self.storage.upsert_position(pos)
            if old:
                report["updated"] += 1
            else:
                report["restored"] += 1
        return report
