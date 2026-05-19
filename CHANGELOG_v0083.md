# v0083 — LIQUIDITY RETEST STRUCTURE CONFIRMATION

Добавлен полноценный SMC-style режим `liquidity_retest` по логике из видео.

Что добавлено:
- Liquidity sweep / grab по swing high / swing low;
- BOS / CHOCH подтверждение после sweep;
- displacement candle filter;
- order block zone;
- FVG / imbalance zone;
- retest зоны перед входом;
- проверка, что структура не сломана на ретесте;
- TP по adaptive RR `2R / 3R / 4R`;
- TP может учитывать ближайшую liquidity target;
- SL остаётся за OB/FVG/zone/wick;
- отдельные настройки:
  - `liquidity_retest_min_displacement_pct`
  - `liquidity_retest_min_displacement_body`
  - `liquidity_retest_min_volume_ratio`
  - `liquidity_retest_min_target_rr`
- режим остаётся manual-only и не входит в `hybrid/all`;
- America bias сохранён: US open 16:30–20:30 MSK даёт SHORT priority, LONG строже;
- scalp tiny TP / aggressive scalp exits не применяются к `liquidity_retest`.

Проверка:
- `104 passed`.
