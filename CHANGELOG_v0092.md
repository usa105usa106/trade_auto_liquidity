# v0092 RUN IMMEDIATE SCAN WAKEUP

- Run button now wakes the scanner immediately instead of waiting `scan_interval_sec`.
- Long scan presets still work as pauses between completed cycles.
- Important settings/menu changes wake the loop so new settings apply immediately.
- Replaced fixed scanner sleeps with an interruptible wakeup event.
- Updated version to `0092 RUN IMMEDIATE SCAN WAKEUP`.
