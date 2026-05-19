# v0097 CLOSE CANCEL BALANCE HARDENING

- Fixed MEXC private cancel symbol normalization: private REST now sends `BASE_USDT` contract ids, not `BASE/USDT`.
- Hardened `/cancel_all` to cover normal, plan, stop, TP/SL cancel-all endpoints and individual order fallback cleanup.
- Hardened `/close_all` cache cleanup: local cache is cleared when exchange positions are flat, even if frozen balance is still stale from orders.
- Improved `/balance` margin display for MEXC raw `positionMargin=0` while real positions are open.
- Added `/ping` response time in ms, current local/exchange position count, and total positions opened counter.
- Added persistent `total_positions_opened` counter.
- Updated `/help` for new diagnostics and cleanup behavior.
