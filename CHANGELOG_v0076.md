# v0077 DEBUG CLEANUP

- Fixed `/open_orders` detection for MEXC futures TP/SL orders.
- Added official current TP/SL endpoint `/api/v1/private/stoporder/open_orders`.
- Fixed MEXC query parameter names: `state`, `is_finished`, `page_num`, `page_size`.
- Split combined TP/SL rows into explicit TP and SL pseudo-orders so protection status can become `EXCHANGE PROTECTED`.
- Improved price parsing for `takeProfitPrice` and `stopLossPrice`.
- Kept private REST pinned to `api.mexc.com`; WebSocket remains unchanged.
