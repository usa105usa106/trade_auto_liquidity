# v0085 — LIQUIDITY RETEST VIDEO MATCH

Реальное усиление отдельного режима `liquidity_retest`, чтобы он ближе повторял SMC/liquidity retest логику из видео.

## Что добавлено

- Higher-timeframe context approximation: бот агрегирует текущие свечи в 5m-like структуру и оценивает MTF bias.
- Zone quality score:
  - order block / FVG наличие;
  - displacement strength;
  - BOS/CHOCH strength;
  - volume confirmation;
  - sweep wick;
  - retest rejection wick.
- Проверка, что зона не была сломана между displacement и retest.
- Проверка rejection wick на ретесте зоны.
- Clean path to liquidity target:
  - по умолчанию не блокирует вход, а влияет на RR/score;
  - можно включить строгий режим через `/set liquidity_retest_require_clean_path true`.
- Adaptive RR теперь учитывает:
  - zone quality;
  - MTF score;
  - clean path;
  - OB/FVG/displacement/BOS как раньше.
- В details сигнала добавлены:
  - `zone_quality`
  - `zone_intact`
  - `mtf_score`
  - `clean_path`
  - `retest_rejection_wick`

## Новые настройки

- `/set liquidity_retest_min_retest_rejection_wick 0.25`
- `/set liquidity_retest_min_zone_quality 2.0`
- `/set liquidity_retest_mtf_enabled true`
- `/set liquidity_retest_min_mtf_score -0.25`
- `/set liquidity_retest_require_clean_path false`

## Важно

100% копия видео невозможна без ручной визуальной интерпретации и точной расшифровки аудио, но v0085 ближе к видео, чем v0084: теперь это не просто sweep → retest, а sweep → BOS/CHOCH → OB/FVG → валидная зона → rejection retest → MTF/context → adaptive RR.

## Проверка

- `pytest`: 110 passed
- `compileall`: OK
