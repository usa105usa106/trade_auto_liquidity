# v0078 SCALP EXIT SAFETY

- Оставлен scalp-профиль TP/SL: TP 0.12–0.30%, SL 0.20–0.45% по умолчанию.
- Добавлен fee-aware фильтр входа: бот не открывает микроскальп, если TP не покрывает оценку комиссий, spread/slippage buffer и минимальный net profit.
- Исправлен double-close race: MEXC `2009 Position is nonexistent or closed` теперь считается нормальным состоянием `already flat`, а не ошибкой сделки.
- Добавлен grace recheck после close через реальные open positions по конкретному символу.
- Убрана зависимость подтверждения close от временного `hidden margin` в account/assets.
- Telegram events больше не показывают технический мусор `HTTP 200`, `2009`, `hidden margin`.
- После close локальный cache чистится, а если биржа всё-таки показывает реальную позицию — она будет восстановлена через sync/positions.
