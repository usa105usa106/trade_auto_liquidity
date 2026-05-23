# v0203 BOOST HUNTER AUTOPILOT

Режим BOOST теперь работает как HUNTER, а не как постоянный скальпер шума.

Главная логика:
- 126 zero-fee монет остаются базовой вселенной.
- Каждые 5 минут строится hotlist самых активных монет.
- Каждые 1-3 секунды deep-scan проверяет только hotlist.
- Вход разрешён только при extreme impulse: движение, ускорение, узкий spread, объём, orderbook imbalance.
- Если условия слабые, бот пишет HUNTER no-trade и не входит.
- Rescue rotation в минус по умолчанию выключен.
- Stop BOOST отключает новые входы, hotlist/rotation/live-panel и сбрасывает cache.

Новые ключевые настройки:
- boost_hunter_mode=true
- boost_hunter_min_score=105
- boost_hunter_extreme_score=145
- boost_hunter_min_accel_pct=0.05
- boost_hunter_min_move_3m_pct=0.22
- boost_hunter_max_wick_pct=0.42
- boost_min_tp_pct=0.18
- boost_max_tp_pct=0.55
- boost_max_spread_pct=0.035
- boost_spot_imbalance_ratio=1.75
- boost_futures_momentum_min_pct=0.14

Цель изменений: меньше мусорных live-входов, меньше overtrading, больше ожидания аномального движения.
