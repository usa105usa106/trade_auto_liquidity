# v0086 SCAN SPEED PRESETS

- Updated bot version to `0086 SCAN SPEED PRESETS`.
- Changed default scan interval from `3s` to `5s`.
- Expanded Scan speed menu presets:
  - `3s`, `5s`, `10s` for scalp modes.
  - `30s`, `1m`, `5m`, `15m`, `30m`, `1h`, `4h` for slower/intraday modes such as `liquidity_retest`.
- Settings screen now renders long scan intervals as `15m`, `30m`, `1h`, `4h` instead of raw seconds.
- `.env.example` default `SCAN_INTERVAL_SEC` updated to `5`.
