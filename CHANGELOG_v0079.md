# v0079 CLEAN POSITION LIFECYCLE + STRATEGY RISK PROFILES

- Fixed post-close event noise: MEXC `2009 Position is nonexistent or closed`, `HTTP 200` API payloads and hidden-margin settlement text are no longer shown in Telegram position events.
- Added terminal CLOSED lifecycle handling: after TP/SL/time-stop closes a symbol, stale callbacks are ignored until cooldown expires.
- Prevented `breakeven moved` events after the same position was already closed.
- Added idempotent close wrapper in PositionManager so duplicate TP/SL/time-stop callbacks do not spam repeat close attempts.
- Fixed Run button UX: first Run press now sends one combined start/status message instead of two messages.
- Kept momentum scalp TP/SL profile unchanged: TP 0.12–0.30%, SL 0.20–0.45%, 5x by settings.
- Added separate configurable TP/SL profiles for pullback and reversal instead of silently using exactly the same bands for every strategy.
- Version bumped to `0079 CLEAN POSITION LIFECYCLE + STRATEGY RISK PROFILES`.
