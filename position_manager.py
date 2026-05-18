import time
import os

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
        self.breakeven_trigger_pct = float(os.getenv("BREAKEVEN_TRIGGER_PCT", "0.30"))

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
        if now - opened >= self.limit_timeout_sec:
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
                    pos["protection_warning"] = "exchange protection failed; local TP/SL monitor active"
                    pos.update(protection)
                    await self.storage.upsert_position(pos)
                    if os.getenv("ALLOW_AUTO_CLOSE_ON_PROTECTION_FAILED", "false").lower() in {"1", "true", "yes", "on"}:
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
            try:
                price=await price_provider(symbol)
            except Exception as e:
                events.append({"type":"price_error","symbol":symbol,"error":str(e)})
                continue
            if not price:
                continue
            side=str(pos.get("side")).upper(); stop=float(pos.get("stop_price") or 0); take=float(pos.get("take_price") or 0); entry=float(pos.get("entry_price") or 0); opened=float(pos.get("opened_at") or now); pnl=self.pnl_pct(pos, price)
            if pnl>=self.breakeven_trigger_pct and entry>0:
                if (side=="LONG" and stop<entry) or (side=="SHORT" and stop>entry):
                    pos["stop_price"]=entry; await self.storage.upsert_position(pos); events.append({"type":"breakeven","symbol":symbol})
                    stop = entry
            if side=="LONG":
                if take and price>=take:
                    events.append({"type":"tp","symbol":symbol,"result":await self.execution_engine.close_position(pos,"take_profit",live,price)}); continue
                if stop and price<=stop:
                    events.append({"type":"sl","symbol":symbol,"result":await self.execution_engine.close_position(pos,"stop_loss",live,price)}); continue
            else:
                if take and price<=take:
                    events.append({"type":"tp","symbol":symbol,"result":await self.execution_engine.close_position(pos,"take_profit",live,price)}); continue
                if stop and price>=stop:
                    events.append({"type":"sl","symbol":symbol,"result":await self.execution_engine.close_position(pos,"stop_loss",live,price)}); continue
            if now-opened>=self.time_stop_sec:
                events.append({"type":"time_stop","symbol":symbol,"result":await self.execution_engine.close_position(pos,"time_stop",live,price)})
        return events
