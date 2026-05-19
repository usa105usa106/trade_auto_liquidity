# v0096 SAFE STOP MARKET RECOVERY

- Changed `/stop` to pause only new entries while the trading loop keeps managing open positions.
- Kept `/panic` as the full emergency stop and close workflow.
- Added a hard market-data gate for new entries when scanner data is stale/weak; open positions are still managed.
- Added a one-time pre-entry recovery checkpoint after each `/run`: live mode reconciles MEXC positions and reattaches protection; paper mode records a no-op recovery checkpoint.
- Updated version to `0096 SAFE STOP MARKET RECOVERY`.
