# v0088 OPENAI SIGNAL HARDENING

- Hardened OpenAI verdict parsing: string false/no/reject is no longer treated as approve.
- Fixed Chat Completions fallback text extraction for plain string message.content.
- Added OpenAI model validation/fallback to gpt-5.4-mini.
- Prevented /set openai_api_key from echoing the full key in Telegram.
- Added regression tests for AI parser, chat fallback, model fallback, and key masking.
