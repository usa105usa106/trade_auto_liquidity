# v0075 MEXC API HOST CLEANUP

- Forced MEXC private REST to `https://api.mexc.com` even if old `.env` still contains `contract.mexc.com`.
- Removed private REST fallback to `contract.mexc.com` for read/sync requests.
- Removed `ccxt.cancel_all_orders(None)` fallback on MEXC because it can call `contract.mexc.com/.../cancel_all` and return CDN 403.
- `/cancel_all` now safely returns skipped when there are no discovered symbols instead of hitting the wrong host.
- `/close_all` and Panic no longer show harmless native `2009 Position is nonexistent or closed` as a failure after listed positions were already closed.
- WebSocket URL remains `wss://contract.mexc.com/edge`; this is separate from private REST trading.
