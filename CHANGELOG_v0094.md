# v0094 LIQUIDITY RETEST RUNNER

- Added real `liquidity_runner_enabled` setting, OFF by default.
- Added Settings button: `Liquidity Runner ON/OFF`.
- Runner applies only to `liquidity_retest` positions.
- It is not scalp trailing: it locks profit by R steps:
  - 2R reached -> stop locks +1R.
  - 3R reached -> stop locks +2R.
  - 4R reached -> stop locks +3R if the trade still runs.
- Existing BE after ~1R stays active.
- Fixed TP/SL logic remains unchanged when runner is OFF.
- Updated version to `0094 LIQUIDITY RETEST RUNNER`.
