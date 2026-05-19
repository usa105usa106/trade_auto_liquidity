# v0091 OPENAI DECISION EDIT MODE

- Changed `openai_show_decisions` semantics:
  - OFF = minimal AI operational issues only (errors/timeouts/invalid response), no normal approve/reject spam.
  - ON = detailed AI decision visibility.
- Detailed ON mode now sends one AI message and edits it from `AI analysis started` to final approve/reject result.
- AI chat messages still use the already returned OpenAI verdict; no extra OpenAI request and no extra AI tokens.
- Updated version to `0091 OPENAI DECISION EDIT MODE`.
