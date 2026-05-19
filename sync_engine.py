import os
import time
from execution_engine import ExecutionEngine
from protection_engine import ProtectionEngine


class SyncEngine:
    def __init__(self, storage, exchange_client):
        self.storage = storage
        self.exchange_client = exchange_client

    def _position_qty(self, p: dict) -> float:
        # Return base-coin amount, not raw contract count. Native MEXC sync
        # supplies `amount` already converted from contracts via contractSize.
        for key in ("amount", "qty", "size"):
            try:
                value = p.get(key)
                if value not in (None, ""):
                    return abs(float(value))
            except Exception:
                pass
        info = p.get("info", {}) if isinstance(p.get("info"), dict) else {}
        raw = p.get("contracts")
        if raw is None:
            raw = info.get("positionAmt") or info.get("holdVol") or info.get("vol")
        try:
            contracts = abs(float(raw or 0))
            cs = p.get("contractSize") or info.get("contractSize") or info.get("contract_size")
            cs_f = float(cs or 0)
            return contracts * cs_f if cs_f > 0 else contracts
        except Exception:
            return 0.0

    def _position_side(self, p: dict) -> str:
        side = str(p.get("side") or p.get("info", {}).get("side") or "").lower()
        if "short" in side or side in {"sell", "2", "3"}:
            return "SHORT"
        return "LONG"

    def _entry_price(self, p: dict) -> float:
        for key in ("entryPrice", "entry_price", "average", "markPrice"):
            try:
                v = p.get(key)
                if v:
                    return float(v)
            except Exception:
                pass
        info = p.get("info", {}) if isinstance(p.get("info"), dict) else {}
        for key in ("openAvgPrice", "holdAvgPrice", "entryPrice"):
            try:
                v = info.get(key)
                if v:
                    return float(v)
            except Exception:
                pass
        return 0.0

    async def _last_price(self, symbol: str, fallback: float) -> float:
        if fallback > 0:
            return fallback
        try:
            ticker = await self.exchange_client.fetch_ticker(symbol)
            return float(ticker.get("last") or ticker.get("close") or ticker.get("bid") or ticker.get("ask") or 0)
        except Exception:
            return 0.0

    def _derived_protection(self, side: str, entry: float) -> tuple[float, float]:
        sl_pct = float(os.getenv("EXTERNAL_SYNC_SL_PCT", "0.006"))
        tp_pct = float(os.getenv("EXTERNAL_SYNC_TP_PCT", "0.012"))
        if entry <= 0:
            return 0.0, 0.0
        if side == "SHORT":
            return entry * (1 + sl_pct), entry * (1 - tp_pct)
        return entry * (1 - sl_pct), entry * (1 + tp_pct)

    async def sync(self, protect: bool = True) -> dict:
        report = {"positions": 0, "orders": 0, "entry": 0, "tp": 0, "sl": 0, "imported_positions": 0, "protected_positions": 0, "warnings": []}
        local_symbols = {p.get("symbol") for p in await self.storage.positions()}
        try:
            positions = await self.exchange_client.fetch_positions()
            active = [p for p in (positions or []) if self._position_qty(p) > 0]
            report["positions"] = len(active)
            for p in active:
                symbol = p.get("symbol") or (p.get("info", {}) if isinstance(p.get("info"), dict) else {}).get("symbol")
                if not symbol or symbol in local_symbols:
                    continue
                side = self._position_side(p)
                entry = await self._last_price(symbol, self._entry_price(p))
                stop_price, take_price = self._derived_protection(side, entry)
                if stop_price <= 0 or take_price <= 0:
                    report["warnings"].append(f"{symbol}: imported without derived protection because entry price is unavailable")
                else:
                    report["warnings"].append(
                        f"{symbol}: external position imported with derived local SL/TP; verify protection on exchange"
                    )
                imported = {
                    "symbol": symbol,
                    "side": side,
                    "status": "open",
                    "entry_price": entry,
                    "qty": self._position_qty(p),
                    "stop_price": stop_price,
                    "take_price": take_price,
                    "strategy": "external_sync",
                    "opened_at": time.time(),
                    "updated_at": time.time(),
                    "external_sync": True,
                    "raw_exchange_position": p,
                }
                await self.storage.upsert_position(imported)
                report["imported_positions"] += 1
                if protect and stop_price > 0 and take_price > 0:
                    protection = await ExecutionEngine(self.storage, self.exchange_client).place_protection_orders(imported, live=True)
                    imported.update(protection)
                    await self.storage.upsert_position(imported)
                    if protection.get("ok"):
                        report["protected_positions"] = report.get("protected_positions", 0) + 1
                    else:
                        report["warnings"].append(f"{symbol}: exchange protection placement failed: {protection}")
        except Exception as e:
            report["warnings"].append(f"positions sync failed: {e}")

        try:
            # Verify existing local open rows too; this catches lost TP/SL after restart
            # even when the position was not newly imported.
            eng = ExecutionEngine(self.storage, self.exchange_client)
            pe = ProtectionEngine(self.exchange_client, eng)
            for lp in await self.storage.positions():
                if str(lp.get("status") or "").lower() != "open":
                    continue
                state = await pe.reconcile(lp, live=True, reattach=protect)
                lp.update(state)
                if state.get("protection_status") == "EXCHANGE PROTECTED":
                    report["protected_positions"] = report.get("protected_positions", 0) + 1
                    lp.pop("protection_warning", None)
                else:
                    lp["protection_warning"] = "exchange TP/SL not fully confirmed; local TP/SL monitor active"
                    report["warnings"].append(f"{lp.get('symbol')}: protection status {state.get('protection_status')}")
                await self.storage.upsert_position(lp)
        except Exception as e:
            report["warnings"].append(f"protection reconcile failed: {e}")

        try:
            orders = await self.exchange_client.fetch_open_orders()
            report["orders"] = len(orders or [])
            for o in orders or []:
                info = o.get("info", {}) if isinstance(o.get("info"), dict) else {}
                cid = str(o.get("clientOrderId") or o.get("client_order_id") or info.get("clientOrderId") or "").lower()
                if "entry" in cid: report["entry"] += 1
                if "tp" in cid or "take" in cid: report["tp"] += 1
                if "sl" in cid or "stop" in cid: report["sl"] += 1
        except Exception as e:
            report["warnings"].append(f"orders sync failed: {e}")
        return report
