# v0082 — LIQUIDITY RETEST ADAPTIVE RR REAL

Реальная интеграция нового режима стратегии `liquidity_retest`.

Что добавлено:
- отдельный режим в меню Strategy: `Liquidity Retest`;
- стратегия включается только вручную через `strategy_mode=liquidity_retest`;
- режим не подключён в `hybrid/all`, чтобы не мешать текущему scalp-боту;
- detection liquidity sweep / liquidity grab;
- detection zone retest после sweep;
- SL строится за liquidity zone / wick с буфером;
- TP считается адаптивно от риска: `2R`, `3R`, `4R`;
- America bias сохранён и переосмыслен для US open dump window `16:30–20:30 МСК`:
  - SHORT setup получает приоритет;
  - LONG setup требует более сильное подтверждение;
- Spot confirmation и Session filter остаются глобальными кнопками и работают для нового режима;
- aggressive scalp trailing/time-stop отключены для `liquidity_retest`;
- оставлен только длинный safety time-stop `LIQUIDITY_RETEST_TIME_STOP_SEC`;
- breakeven для `liquidity_retest` переносится не по scalp-проценту, а после движения примерно `1R`;
- добавлены runtime `/set` параметры:
  - `liquidity_retest_default_rr`
  - `liquidity_retest_sl_buffer_pct`
  - `liquidity_retest_time_stop_sec`.

Проверка:
- добавлены регрессионные тесты v0082;
- полный pytest: `101 passed`.
