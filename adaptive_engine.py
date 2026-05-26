from collections import defaultdict

STRATEGIES = ["momentum", "pullback", "reversal", "liquidity_retest", "quick_bounce", "impulse_dump", "orderflow_impulse", "cascade_hunter", "strongest_coin", "hybrid", "all"]

def safe_div(a, b, default=0.0):
    return a / b if b else default

class AdaptiveEngine:
    def calc_stats(self, trades: list[dict]) -> dict:
        by_strategy = defaultdict(list)
        normal, mirror = [], []
        for t in trades:
            by_strategy[str(t.get("strategy","unknown")).lower()].append(t)
            (mirror if t.get("mirror_used") else normal).append(t)

        def metrics(rows):
            wins = [float(x.get("pnl_usdt") or 0) for x in rows if float(x.get("pnl_usdt") or 0) > 0]
            losses = [abs(float(x.get("pnl_usdt") or 0)) for x in rows if float(x.get("pnl_usdt") or 0) < 0]
            pnl = sum(wins) - sum(losses)
            total = len(rows)
            gross_win = sum(wins)
            gross_loss = sum(losses)
            if gross_loss > 0:
                pf = gross_win / gross_loss
                pf_display = f"{pf:.2f}"
            elif gross_win > 0:
                # No losses yet. Mathematically PF is infinite/undefined, not 999.
                # Keep a capped numeric value for old strategy scoring, but expose
                # a human-readable value for Telegram stats.
                pf = 10.0
                pf_display = "∞"
            else:
                pf = 0.0
                pf_display = "-"
            return {
                "trades": total,
                "wins": len(wins),
                "losses": len(losses),
                "winrate": safe_div(len(wins), total, 0)*100,
                "pnl": pnl,
                "profit_factor": pf,
                "profit_factor_display": pf_display,
                "expectancy": safe_div(pnl, total, 0),
                "drawdown_proxy": gross_loss,
            }

        stats = {"strategies": {k: metrics(v) for k,v in by_strategy.items()}}
        stats["normal"] = metrics(normal)
        stats["mirror"] = metrics(mirror)
        stats["normal_profit_factor"] = stats["normal"]["profit_factor"]
        stats["mirror_profit_factor"] = stats["mirror"]["profit_factor"]
        stats["normal_expectancy"] = stats["normal"]["expectancy"]
        stats["mirror_expectancy"] = stats["mirror"]["expectancy"]
        return stats

    def choose_strategy(self, base_mode: str, trades: list[dict], regime: str = "LOW_VOLATILITY", enabled: bool = True) -> str:
        base_mode = (base_mode or "hybrid").lower()
        if base_mode not in STRATEGIES:
            base_mode = "hybrid"
        # "all" is an explicit mode: scan every strategy every cycle.
        # "hybrid" is adaptive: choose one effective strategy by market regime/stats.
        if base_mode == "all":
            return "all"
        if not enabled:
            return base_mode
        if base_mode != "hybrid":
            return base_mode

        stats = self.calc_stats(trades).get("strategies", {})
        regime_defaults = {
            "TRENDING": "momentum",
            "CHOPPY": "reversal",
            "HIGH_VOLATILITY": "pullback",
            "LOW_VOLATILITY": "pullback",
        }
        default = regime_defaults.get(regime, "pullback")
        if not stats:
            return default

        regime_bias = {
            "TRENDING": {"momentum": 8, "pullback": 2, "reversal": -4},
            "CHOPPY": {"reversal": 8, "pullback": 2, "momentum": -4},
            "HIGH_VOLATILITY": {"pullback": 5, "momentum": 3, "reversal": -2},
            "LOW_VOLATILITY": {"pullback": 5, "reversal": 2, "momentum": 0},
        }.get(regime, {})

        best, best_score = default, -10**9
        for name, m in stats.items():
            if name not in {"momentum", "pullback", "reversal"}:
                continue
            # Avoid overfitting tiny samples. Until enough data exists, regime default wins.
            if m["trades"] < 10:
                continue
            score = (
                m["profit_factor"] * 10
                + m["expectancy"] * 2
                + m["winrate"] * 0.05
                - m["drawdown_proxy"] * 0.01
                + regime_bias.get(name, 0)
            )
            if score > best_score:
                best, best_score = name, score
        return best
