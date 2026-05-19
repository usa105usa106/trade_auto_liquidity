# v0087 OPENAI SIGNAL CONFIRMATION

- Added real OpenAI-only signal confirmation module; Ollama is not used.
- Added `openai_signal_engine.py` with compact token-saving prompts.
- Added separate prompts for scalp/momentum-style setups and `liquidity_retest` SMC setups.
- Added Weak / Medium / Strong verification modes.
- Default model is `gpt-5.4-mini` for all strategies.
- Added Settings menu button: `🤖 ИИ анализ`.
- Added model selection menu: `gpt-5.4-mini`, `gpt-4o-mini`, `gpt-5.5`, `gpt-5.5-pro`, `gpt-4.1`.
- Added `/openai status|set KEY|clear|test` command.
- Integrated AI gate into the autotrade pipeline before order execution:
  - approve -> trade can open;
  - reject -> setup is skipped and scanner continues.
- AI fail-closed by default when enabled and key/API is missing; optional `openai_fail_open` setting exists.
- Added runtime settings: `openai_analysis_enabled`, `openai_model`, `openai_check_strength`, `openai_api_key`, `openai_env_fallback`, `openai_timeout_sec`, `openai_fail_open`.
