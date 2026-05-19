# v0084 LIQUIDITY RETEST RUNTIME SETTINGS FIX

- Audited v0083 after adding the Liquidity Retest / SMC mode.
- Fixed runtime settings bug: `/set liquidity_retest_*` values stored in SQLite now affect the live scanner immediately without restarting the bot.
- Added runtime wiring for Liquidity Retest thresholds:
  - `liquidity_retest_zone_tolerance_pct`
  - `liquidity_retest_min_sweep_wick`
  - `liquidity_retest_min_reclaim_pct`
  - `liquidity_retest_min_displacement_pct`
  - `liquidity_retest_min_displacement_body`
  - `liquidity_retest_min_volume_ratio`
  - `liquidity_retest_min_target_rr`
  - `liquidity_retest_max_spread_pct`
- Added those missing keys to defaults and `/set` allow-list.
- Also fixed runtime wiring for weak-momentum filter settings so `/set` changes apply without restart.
- Kept Liquidity Retest manual-only and not included in hybrid/all.
- Tests: 106 passed.
