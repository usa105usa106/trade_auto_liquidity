# v0102 MEXC CONTRACT SYMBOLS EVERYWHERE

- Enforces native MEXC futures contract symbols (`BTC_USDT`, `ETH_USDT`) at the private REST request boundary.
- Adds `mexc_contract_symbol()` helper and normalizes every private `body/query.symbol` before signing.
- Defaults AI scalping symbols to `BTC_USDT,ETH_USDT`.
- Keeps ccxt/display symbols internally where needed for market data, but never sends `BTC/USDT` to MEXC private endpoints.
- Adds regression tests for symbol normalization and request payload safety.
