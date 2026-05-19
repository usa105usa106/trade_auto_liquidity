# v0090 OPENAI DECISION VISIBILITY

- Added `openai_show_decisions` toggle.
- Added Telegram AI lifecycle messages when the toggle is ON:
  - AI analysis started;
  - AI approved setup;
  - AI rejected setup.
- Messages include symbol, side, strategy, model, strength, confidence, and short reason.
- Telegram visibility uses the already received OpenAI verdict and does not create extra OpenAI calls or spend extra tokens.
- Default remains OFF to avoid Telegram spam.
