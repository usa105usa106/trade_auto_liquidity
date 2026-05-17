import time

class RiskEngine:
    def __init__(self, storage):
        self.storage = storage

    async def daily_pnl(self) -> float:
        since = time.time() - 86400
        trades = await self.storage.trade_rows(since)
        return sum(float(t.get("pnl_usdt") or 0) for t in trades)

    async def loss_streak(self) -> int:
        trades = await self.storage.trade_rows()
        streak = 0
        for t in reversed(trades):
            if float(t.get("pnl_usdt") or 0) < 0: streak += 1
            else: break
        return streak

    async def allow_new_trades(self, settings: dict, equity: float = 1000.0) -> tuple[bool, str]:
        max_loss_pct = float(settings.get("max_daily_loss_pct", 3.0))
        pnl = await self.daily_pnl()
        if equity and pnl <= -(equity * max_loss_pct/100):
            return False, "daily loss limit reached"
        if await self.loss_streak() >= int(settings.get("max_consecutive_losses", 4)):
            return False, "consecutive loss pause"
        return True, "ok"

    def market_filters(self, candidate: dict, settings: dict) -> tuple[bool, str]:
        spread = float(candidate.get("spread_pct", 0.0))
        slippage = float(candidate.get("expected_slippage_pct", 0.0))
        depth = float(candidate.get("depth_usdt", 10**9))
        spread_limit = float(settings.get("max_spread_pct", 0.20)) * float(candidate.get("spread_limit_multiplier", 1.0))
        if spread > spread_limit: return False, "spread too high"
        if slippage > float(settings.get("max_slippage_pct", 0.20)): return False, "slippage risk"
        if depth < float(settings.get("min_depth_usdt", 5000)): return False, "weak depth"
        return True, "ok"
