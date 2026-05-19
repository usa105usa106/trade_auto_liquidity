# v0089 OPENAI PROMPT QUALITY FIX

- Strengthened OpenAI AI-gate prompts for real trading use.
- Added richer compact payload for scalp signals: move_1m, move_5m, breakout, volume, spread, slippage, orderbook imbalance, TP/SL distance.
- Added richer compact payload for liquidity_retest: zone_type, zone_intact, target_rr, sweep/retest wick, BOS strength, FVG bounds, clean_path, MTF score.
- Prompts now explicitly say that bot-calculated Entry/SL/TP must not be changed by AI.
- Added confidence floor by AI strength:
  - weak: 0.55
  - medium: 0.65
  - strong: 0.75
- AI approvals below the selected confidence floor are rejected automatically.
- Kept token usage compact and JSON-only.
