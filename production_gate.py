class ProductionGate:
    """
    Pre-trade safety gate.
    Blocks NEW entries only. It must never stop position management.
    """

    def _ws_enabled(self, settings: dict) -> bool:
        return bool(settings.get("ws_enabled", settings.get("websocket_enabled", True)))

    def validate_for_live(self, settings: dict, api_ready: bool, ws_healthy: bool, sync_ok: bool) -> tuple[bool, str]:
        if not bool(settings.get("live_trading", False)):
            return False, "live_trading is OFF"
        if not api_ready:
            return False, "exchange API is not ready"
        if self._ws_enabled(settings) and bool(settings.get("ws_require_healthy_for_entries", True)) and not ws_healthy:
            return False, "websocket is unhealthy/stale"
        if not sync_ok:
            return False, "exchange sync is not OK"
        if float(settings.get("risk_pct", 0.0)) <= 0:
            return False, "risk_pct must be > 0"
        if int(settings.get("max_open_positions", 0)) <= 0:
            return False, "max_open_positions must be > 0"
        return True, "ok"

    def validate_for_paper(self, settings: dict, ws_healthy: bool = True) -> tuple[bool, str]:
        # Paper mode may run even without private exchange API.
        if self._ws_enabled(settings) and bool(settings.get("ws_require_healthy_for_entries", True)) and not ws_healthy:
            return False, "websocket is unhealthy/stale"
        if float(settings.get("risk_pct", 0.0)) <= 0:
            return False, "risk_pct must be > 0"
        if int(settings.get("max_open_positions", 0)) <= 0:
            return False, "max_open_positions must be > 0"
        return True, "ok"
