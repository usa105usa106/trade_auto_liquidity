# v0104 AI SCALPING RECOVERY OPENAI

- AI BTC/ETH scalping mode now auto-enables OpenAI when selected via `/set strategy_mode ai_scalping`, not only via the main menu button.
- Recovery after restart now recalculates recovered BTC/ETH TP/SL from the AI scalping settings when `strategy_mode=ai_scalping` and local plan data is missing.
- Recovered BTC/ETH scalping positions are marked with `strategy=ai_scalping` and `recovery_tp_sl_source=ai_scalping_btc|ai_scalping_eth`.
- Version and env example updated to v0104.
- Tests updated and passed.
