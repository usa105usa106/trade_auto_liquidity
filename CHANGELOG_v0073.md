# v0073 SETTINGS RUNTIME SYNC + POSITION SAFETY

Проверка и исправления после ревизии кода v0072.

## Что исправлено
- `/set require_exchange_protection true|false` теперь реально влияет на `place_protection_orders`, а не только сохраняется в SQLite.
- `/set auto_close_on_protection_failed true|false` теперь читается из SQLite в live-entry и pending-limit fill workflow.
- `/set limit_timeout_sec` теперь применяется к pending limit позициям без рестарта и без ENV.
- `/set cooldown_after_close_sec` теперь применяется при постановке lock после закрытия позиции.
- Добавлены дефолты в SQLite для `time_stop_sec`, `breakeven_trigger_pct`, `require_exchange_protection`, `auto_close_on_protection_failed`.
- Добавлены регрессионные тесты на runtime-настройки и защитные сценарии позиций.

## Проверено
- `python -m compileall -q .`
- `python -m pytest -q` → 71 passed
