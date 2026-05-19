# v0077 DEBUG CLEANUP

- Cleaned `/mexc_debug_state` output: no more 404/ERR spam from invalid MEXC debug endpoints.
- Removed unsupported debug probes:
  - `/api/v1/private/position/list/open_positions`
  - `/api/v1/private/position/holding`
- Kept only working MEXC diagnostic endpoints:
  - `/api/v1/private/account/assets`
  - `/api/v1/private/position/open_positions`
  - `/api/v1/private/order/list/open_orders`
  - `/api/v1/private/planorder/list/orders`
  - `/api/v1/private/stoporder/list/orders`
- Suppressed read errors inside debug collection so Telegram output remains clean.
- Position sync now uses the valid `open_positions` endpoint only.
