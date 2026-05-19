# v0093 TRADE SETUP CHARTS

- Added optional trade setup charts, disabled by default.
- Added Settings toggle: `📊 Графики сделок` / `trade_charts_enabled`.
- When enabled, bot sends one clear PNG chart only after an auto trade is opened.
- Chart includes entry point, stop-loss red risk window, max take-profit green reward window, and optional liquidity zone.
- Chart generation is local matplotlib rendering and does not spend OpenAI tokens.
- Added `.env.example` options: `TRADE_CHARTS_ENABLED`, `TRADE_CHART_TIMEFRAME`, `TRADE_CHART_CANDLE_LIMIT`.
