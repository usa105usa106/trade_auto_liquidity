# v0081 SMART SCALP EXITS

- Added `scalp_exit_engine.py` with one central local scalp exit policy.
- Breakeven now defaults to `+0.12%` raw price profit with a tiny `+0.01%` offset instead of waiting for `+0.30%`.
- Added smart time-stop logic:
  - exits stale/no-progress scalps earlier;
  - allows limited extension when the trade is already working in profit;
  - keeps the hard fallback timeout.
- Added trailing scalp exit:
  - tracks best local PnL;
  - closes after configurable giveback from the best profit.
- Added weak momentum suppression:
  - 1m impulse alone is not enough;
  - requires 5m confirmation, acceptable spread and breakout/orderbook support.
- Added runtime settings for all new scalp-exit knobs via `/set`.
- Updated version to `0081 SMART SCALP EXITS`.
- Added regression tests for breakeven, trailing exit, smart time-stop and weak momentum filter.

Tests: `97 passed`.
