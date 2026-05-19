# v0080 PROTECTION PRECISION + LOCAL BOT FALLBACK

- Fixed MEXC futures symbol normalization for native endpoints: display symbols like `ONDO/USDT:USDT` are converted to contract ids like `ONDO_USDT` before private REST calls.
- Added centralized TP/SL/quantity precision sanitizing before storage and before exchange protection orders. This removes float tails such as `0.38182571428571427` and prevents MEXC `2015 Price or quantity precision error` where market metadata is available.
- Exchange TP/SL placement now receives the same rounded values that `/positions` displays.
- Changed missing exchange TP/SL state from scary `UNPROTECTED`/`LOCAL FALLBACK` to `LOCAL BOT PROTECTED`: the bot keeps the position and monitors TP, SL, breakeven and time-stop locally.
- Cleaned `/positions` output: raw HTTP/MEXC errors are kept in raw state/debug data but no longer shown in the normal Telegram positions screen.
- Recovery/sync now removes duplicate local aliases for the same exchange contract and keeps exchange quantity as source of truth.
- `/close_all` no longer surfaces stale hidden-margin text as a user-facing failure.
- Added v0080 regression tests for symbol normalization, precision sanitizer presence, and local fallback protection status.

Tests: `93 passed`.
