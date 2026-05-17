class MirrorEngine:
    def __init__(self, mode: str = "off"):
        self.mode = (mode or "off").lower()

    def should_mirror(self, stats: dict | None = None) -> bool:
        if self.mode == "on":
            return True
        if self.mode == "off":
            return False
        stats = stats or {}
        normal_pf = float(stats.get("normal_profit_factor", 1.0) or 1.0)
        mirror_pf = float(stats.get("mirror_profit_factor", 0.0) or 0.0)
        mirror_expectancy = float(stats.get("mirror_expectancy", 0.0) or 0.0)
        normal_expectancy = float(stats.get("normal_expectancy", 0.0) or 0.0)
        return mirror_pf > normal_pf * 1.15 and mirror_expectancy > normal_expectancy

    def apply(self, candidate: dict, stats: dict | None = None) -> dict:
        c = dict(candidate)
        if self.should_mirror(stats):
            c["original_side"] = c.get("side")
            c["side"] = "SHORT" if str(c.get("side")).upper() == "LONG" else "LONG"
            c["mirror_used"] = True
            c["confidence"] = float(c.get("confidence",0)) - 2
        else:
            c["mirror_used"] = False
        return c
