# v0098 AI BTC ETH SCALPING LOOP

- Added `strategy_mode=ai_scalping`.
- Bot asks OpenAI for BTC/ETH direction only when no active local/exchange position exists.
- OpenAI controls only `symbol`, `LONG/SHORT/WAIT`, `confidence`, and short reason.
- Bot keeps execution control: size, leverage, TP, SL, risk gates, protection orders, retries, close/recovery.
- Added low-token compact market snapshot: 1m/5m EMA trend, RSI, ATR%, spread, top-book depth and imbalance.
- Added settings: `ai_scalping_symbols`, `ai_scalping_min_confidence`, `ai_scalping_tp_pct`, `ai_scalping_sl_pct`, `ai_scalping_max_spread_pct`.
- Updated `/help`, `/set`, and strategy menu.
