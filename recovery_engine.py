from __future__ import annotations

import os
import time
from typing import Any

from protection_engine import ProtectionEngine


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

    def _fallback_tp_sl(self, ex_pos: dict, settings: dict | None = None) -> tuple[float, float, str]:
        """Fallback TP/SL for exchange-only recovered rows.

        v0104: when the bot is in AI BTC/ETH scalping mode and the local
        SQLite row was lost, recovered BTC/ETH positions must use the same
        deterministic scalping TP/SL as fresh entries. Otherwise a restart can
        silently reattach wider generic recovery levels.
        """
        entry = self._entry(ex_pos)
        if entry <= 0:
            return 0.0, 0.0, "none"
        settings = settings or {}
        side = self._side(ex_pos)
        keys = self._keys(ex_pos)
        symbol_key = " ".join(sorted(keys)).upper()
        scalping_mode = str(settings.get("strategy_mode", "")).lower() == "ai_scalping"

        if scalping_mode and "BTC_USDT" in symbol_key:
            tp_pct = float(settings.get("ai_scalping_btc_tp_pct", os.getenv("AI_SCALPING_BTC_TP_PCT", "0.18")) or 0.18) / 100.0
            sl_pct = float(settings.get("ai_scalping_btc_sl_pct", os.getenv("AI_SCALPING_BTC_SL_PCT", "0.26")) or 0.26) / 100.0
            source = "ai_scalping_btc"
        elif scalping_mode and "ETH_USDT" in symbol_key:
            tp_pct = float(settings.get("ai_scalping_eth_tp_pct", os.getenv("AI_SCALPING_ETH_TP_PCT", "0.22")) or 0.22) / 100.0
            sl_pct = float(settings.get("ai_scalping_eth_sl_pct", os.getenv("AI_SCALPING_ETH_SL_PCT", "0.32")) or 0.32) / 100.0
            source = "ai_scalping_eth"
        else:
            sl_pct = float(os.getenv("RECOVERY_LOCAL_SL_PCT", os.getenv("MAX_SL_PCT", "0.60")) or 0.60) / 100.0
            tp_pct = float(os.getenv("RECOVERY_LOCAL_TP_PCT", os.getenv("MIN_TP_PCT", "0.25")) or 0.25) / 100.0
            source = "generic_recovery"

        if side == "SHORT":
            return entry * (1 + sl_pct), entry * (1 - tp_pct), source
        return entry * (1 - sl_pct), entry * (1 + tp_pct), source

    async def _protection_state(self, pos: dict) -> dict:
        return await ProtectionEngine(self.exchange_client, self.execution_engine).check(pos)

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
        try:
            settings = await self.storage.all_settings()
        except Exception:
            settings = {}
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
            recovery_source = "local"
            if not stop_price or not take_price:
                stop_price, take_price, recovery_source = self._fallback_tp_sl(ep, settings)
            pos = dict(old or {})
            strategy = pos.get("strategy") or ("ai_scalping" if recovery_source.startswith("ai_scalping") or str(settings.get("strategy_mode", "")).lower() == "ai_scalping" else "recovered_exchange")
            liq_mode = bool(settings.get("ai_scalping_liquidation_stop_mode")) and strategy == "ai_scalping"
            pos.update({
                "symbol": symbol,
                "mexc_symbol": ep.get("mexc_symbol") or (ep.get("info") or {}).get("symbol"),
                "symbol_variants": sorted(keys),
                "side": side,
                "status": "open",
                "entry_price": entry or pos.get("entry_price"),
                "qty": qty or pos.get("qty"),
                "exchange_contracts": ep.get("contracts") or ((ep.get("info") or {}).get("holdVol") if isinstance(ep.get("info"), dict) else None),
                "contractSize": ep.get("contractSize") or ((ep.get("info") or {}).get("contractSize") if isinstance(ep.get("info"), dict) else None),
                "stop_price": float(stop_price or 0),
                "take_price": float(take_price or 0),
                "strategy": strategy,
                "liquidation_stop_mode": liq_mode,
                "recovery_tp_sl_source": recovery_source,
                "opened_at": pos.get("opened_at") or time.time(),
                "updated_at": time.time(),
                "exchange_recovered": True,
                "raw_exchange_position": ep,
            })
            try:
                pos = self.execution_engine._sanitize_position_for_exchange(pos)
            except Exception:
                pass
            state = await ProtectionEngine(self.exchange_client, self.execution_engine).reconcile(pos, live=True, reattach=reattach)
            pos.update(state)
            if state.get("protection_status") == "EXCHANGE PROTECTED":
                pos["protection_mode"] = "exchange"
                pos.pop("protection_warning", None)
                report["protection_ok"] += 1
                if state.get("reattach_attempted"):
                    report["reattach_attempted"] += 1
                    report["reattach_ok"] += 1
            else:
                pos["protection_mode"] = "local_monitoring"
                pos["protection_warning"] = "exchange TP/SL not confirmed; bot monitors TP/SL locally"
                report["local_monitoring"] += 1
                if reattach:
                    report["reattach_attempted"] += 1
                    if state.get("reattach_error") or state.get("tp_error") or state.get("sl_error"):
                        report["errors"].append(f"reattach {symbol}: {state.get('reattach_error') or state.get('tp_error') or state.get('sl_error')}")
            pos = self.execution_engine._decorate_position_metrics(pos)
            try:
                if old and old.get("symbol") and old.get("symbol") != pos.get("symbol"):
                    await self.storage.remove_position(old.get("symbol"))
                # Remove duplicate local aliases for the same exchange contract.
                for lp2 in await self.storage.positions():
                    if lp2.get("symbol") != pos.get("symbol") and (self._keys(lp2) & keys):
                        await self.storage.remove_position(lp2.get("symbol"))
            except Exception:
                pass
            await self.storage.upsert_position(pos)
            if old:
                report["updated"] += 1
            else:
                report["restored"] += 1
        return report
