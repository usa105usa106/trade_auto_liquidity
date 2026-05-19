# v0074 EXCHANGE PROTECTION RECOVERY

- Added real TP/SL classification from MEXC open/plan/stop/TP-SL endpoints.
- Added protection status: EXCHANGE PROTECTED, LOCAL FALLBACK, UNPROTECTED.
- /positions now attempts protection recovery during exchange-first reconcile.
- RecoveryEngine re-checks exchange protection and reattaches missing TP/SL.
- SyncEngine reconciles protection for already-local positions, not only newly imported exchange rows.
- place_protection_orders stores protection_mode/protection_status for later rendering.
- /positions output now shows TP/SL confirmed flags and concrete protection status.
