## v0184 BOOST LIVE PANEL EXCHANGE PNL FIX

- Restored fast BOOST live panel as one bottom Telegram message.
- Full zero-fee universe scan stays aggressive.
- Rotation can close only profitable positions and immediately open stronger candidate.
- Micro TP remains 0.03-0.05%.
- Scan/rotation loop interval defaults to 1 sec.

## v0181 BOOST LIVE TPSL SAFE EXECUTION

Preloaded 0-fee whitelist from user screenshots. `/boost_list` can still replace it and `/boost_list_del` clears it.

## v0181 BOOST LIVE TPSL SAFE EXECUTION

- `/boost_start` starts autopilot: reserves `BOOST_BALANCE_SHARE=0.10` of current futures equity as the BOOST bank.
- Target is `BOOST_TARGET_MULTIPLIER=20`; example 50 USDT balance -> 5 USDT bank -> 100 USDT target bank.
- Sizing compounds only the BOOST bank (`bank + closed BOOST PnL`), not the full account.
- It scans API-verified zero-fee futures, chooses the most active symbol, direction and leverage automatically, then rescans after each closed trade.
- `/boost_status` shows current bank, target and session PnL. `/boost_stop` disables new BOOST entries.
- High risk: this mode can lose the entire BOOST bank. No profit is guaranteed.

## v0170 BOOST ZERO FEE SCANNER

- Added `boost_scalping` strategy mode.
- Scans API-verified 0% fee MEXC futures symbols first; if fees cannot be verified it stays idle by default.
- Uses `BOOST_BALANCE_SHARE=0.10`, so a 50 USDT account starts a session bank around 5 USDT.
- Session target is `BOOST_TARGET_MULTIPLIER=20`, so 5 USDT bank target is 100 USDT.
- Stops on target, loss limit, or consecutive losses. This is a high-risk booster mode, not guaranteed profit.

## v0170 BOOST ZERO FEE SCANNER

- Default `AI_SCALPING_MIN_CONFIDENCE` is now `0.72` for aggressive BTC/ETH micro-scalping.
- BTC/ETH scalping TP is dynamic: BTC `0.08–0.12%`, ETH `0.10–0.16%`, based on setup strength.
- SL is dynamic too: `SL = TP * AI_SCALPING_SL_TP_MULTIPLIER` with default multiplier `2.0`.
- Separate position-management loop remains enabled by default for faster local TP/SL handling.

## v0170 BOOST ZERO FEE SCANNER
- Fixed false LOCAL PROTECTION MODE when MEXC native stoporder is active.
- Native stoporder rows with state=1/isFinished=0/errorCode=0 and TP/SL prices are accepted even if local generic id differs from MEXC positionId.
- Generic local pos["id"] is no longer used as exchange positionId.

# v0130 AI scalping TP/SL protection fix

- MEXC native position TP/SL now opens position first, waits for positionId/holdVol, then sends `/stoporder/place`.
- TP/SL trigger prices are moved away from mark/last by tick/min-distance before sending, to avoid too-close trigger rejects.
- AI scalping no longer uses breakeven/trailing/time-stop by default; local manager closes only on TP or SL while watchdog retries exchange TP/SL.
- Set `AI_SCALPING_MANAGE_ONLY_TPSL=0` to restore old BE/trailing/time-stop behavior.

# Liquidity Telegram Bot v0067 SAFE LOCAL STATE + HIDDEN MARGIN

Signal-engine build for Railway with real futures-first candidate generation.

## Что включено

- Telegram bot с обычным главным меню 2 столбика.
- Inline-настройки только внутри Settings.
- Сохранение настроек в SQLite.
- Защита от старых inline callback events через revision.
- Paper/Live mode. По умолчанию Live OFF.
- MEXC/BingX-ready через CCXT. Основной execution venue: MEXC futures.
- Futures-first architecture.
- Spot confirmation только после futures-сигнала.
- Adaptive Strategy Adaptation.
- Market Regime Adaptation.
- Mirror Mode OFF/ON/AUTO.
- Asia/America session engine по МСК.
- Asia: 03:00–07:00 МСК.
- America: 16:30–20:30 МСК.
- America Short Bias: SHORT не режется по риску, LONG режется и требует сильнее сигнал.
- Risk filters: spread, slippage, weak depth.
- Trade journal SQLite.
- /stats: PF, winrate, expectancy.
- /sync: позиции и ордера.
- Proxy settings.
- Railway files: Procfile, railway.json.

## Главное меню

Run / Stop  
Status / Panic  
Positions / Stats  
Balance / Ping  
Settings

## Команды

/start  
/help  
/run  
/stop  
/panic  
/status  
/ping  
/balance  
/positions  
/stats  
/sync  
/proxy on|off|set URL|test  
/set key value

## Важное

Live trading требует:
1. TELEGRAM_TOKEN
2. API ключи биржи
3. LIVE_TRADING=true через Settings или env
4. Проверку на маленьком депозите

Даже с production-safe архитектурой бот не гарантирует прибыль.


## v0025 SIGNAL

Добавлен реальный signal engine.

Теперь scanner не пустой:
- берёт hot futures symbols
- получает OHLCV 1m
- получает orderbook
- ищет реальные кандидаты:
  - Momentum breakout
  - Pullback reclaim
  - Reversal / liquidity sweep
- считает confidence
- отдаёт только кандидатов выше threshold

Пайплайн:
Futures market data
→ SignalEngine
→ Mirror
→ Session engine
→ Spot confirmation
→ Risk filters
→ Execution layer

Важно:
Это не гарантия прибыли. Это реальный генератор сигналов, который всё равно надо paper-forward тестировать.


## v0026 EXECUTION FIX

Исправлено главное:
- signal engine больше не заканчивается no-op
- candidate теперь превращается в TradePlan
- считается qty через risk sizing
- рассчитываются SL/TP через ATR%
- сделка реально отправляется в ExecutionEngine
- paper mode создаёт позицию в SQLite
- live mode отправляет order через CCXT
- PositionManager сопровождает TP/SL/time-stop/breakeven
- spot confirmation запрашивается только после futures candidate



## v0027 WS HARDENING

Добавлено:
- WebSocketSupervisor
- Binance Futures miniTicker realtime stream
- auto reconnect с exponential backoff
- heartbeat / ping-pong
- stale-data protection
- resubscribe через reconnect
- shared ticker cache для price/status
- fail-safe: если WS stale/unhealthy, новые входы блокируются
- positions management продолжает работать через REST fallback
- /status и /ping показывают состояние WS
- Settings: 🔌 WebSocket ON/OFF

Новые настройки:
- ws_enabled
- ws_require_healthy_for_entries
- ws_stale_sec

Важно:
WebSocket hardening снижает риск торговли по старым данным, но не гарантирует прибыль.


## v0028 PRODUCTION REVIEW

Добавлено:
- ProductionGate
- блокировка live-входов без API-ключей
- WebSocket unhealthy/stale больше не блокирует scanner/execution при свежих REST/cache данных
- блокировка новых входов при sync failure
- тесты production gate
- полный audit.py

Важно:
Этот билд технически проверен, но live-edge-cases MEXC/BingX всё равно проверяются только реальными API:
partial fills, reduceOnly, trigger TP/SL, private WS fills.

Потенциальные подозрительные маркеры, найденные статическим поиском:
[]

## v0029 AUTOPILOT HARDENED

Исправлены блокеры полного автопилота:

- position management теперь выполняется первым и не блокируется production gate;
- новые входы блокируются отдельно от сопровождения уже открытых позиций;
- paper mode больше не зависит от private exchange endpoints;
- live mode блокирует вход, если не удалось проверить open orders на бирже;
- добавлен per-symbol async lock против дублей при одновременных сигналах;
- open + pending + closing позиции считаются занятыми слотами;
- pending limit получает lifecycle;
- ProductionGate больше не использует WebSocket как hard gate для входов;
- добавлены тесты autopilot hardening.

## v0030 AUTOPILOT FIXED

Исправлены 7 пунктов ревью:

- pytest теперь запускает async-тесты через `pytest-asyncio`, добавлен `pytest.ini`;
- orderbook больше не подменяется искусственно хорошими значениями: без реального стакана сигнал не создаётся;
- `fetch_positions` больше не возвращает пустой список при отсутствии поддержки биржи, а явно сообщает ошибку;
- live limit-entry больше не считается исполненным только потому, что исчез из open orders: статус подтверждается через `fetch_order`;
- live TP/SL protection теперь обязательна по умолчанию через `REQUIRE_EXCHANGE_PROTECTION=true`; если protection не поставлена, позиция закрывается fail-safe;
- `/run` больше не создаёт параллельные trading loops;
- символы нормализуются через `load_markets()` под swap/futures формат выбранной ccxt-биржи;
- скрытые `except/pass` в торговом контуре заменены на логирование или явное сохранение ошибки.

Проверки текущего архива:

```text
python -m pytest -q
20 passed
```

## v0033 AUTOPILOT HARDENED

Исправлены повторно найденные live/autopilot edge cases:

- `/panic` закрывает локальные позиции и в paper mode, и в live mode;
- ошибка `cancel_all_orders()` больше не прерывает закрытие позиций в `/panic`;
- `/sync` при импорте внешней позиции рассчитывает SL/TP и пытается сразу поставить exchange-side protection orders;
- market entry теперь использует фактическую fill/average цену из ответа биржи и пересчитывает SL/TP от неё;
- Settings callback-кнопки после toggle/set возвращают обновлённое меню, а не тупик “saved”;
- `VERSION` обновлён до `0033 AUTOPILOT HARDENED`;
- старые misleading-комментарии в runtime-коде заменены на актуальные описания.

Проверки текущего архива:

```text
python -m pytest -q
30 passed

python audit.py
AUDIT PASSED

python -m compileall -q .
OK
```


## v0034 ADAPTIVE REGIME WIRED

Исправлено после ревью:

- `VERSION` обновлён до `0034 ADAPTIVE REGIME WIRED`;
- regression-тест версии обновлён и больше не ожидает `0033`;
- `adaptive` universe mode реально меняет число символов по market regime, volatility и ticker breadth вместо простого `max_symbols`;
- `regime_adaptation` определяет market regime по BTC/USDT OHLCV и breadth тикеров;
- `auto_strategy_adaptation` выбирает effective strategy для скана по regime и статистике закрытых сделок;
- Scanner candidates включают `market_regime` и `effective_strategy_mode` metadata.

Проверки текущего архива:

```text
python -m pytest -q
35 passed

python audit.py
AUDIT PASSED

python -m compileall -q .
OK
```


## v0059 MEXC API HOST + RISK FILTERS

- Updated displayed bot version.
- Includes MEXC 403 order fallback, Telegram MEXC settings, recvWindow defaults, isolated mode, and time-difference adjustment.


## v0059 notes
- Futures private REST base defaults to `https://api.mexc.com` per MEXC support recommendation.
- Private MEXC requests are limited to 4 requests per 2 seconds.
- Raw order endpoint remains `/api/v1/private/order/create`; `contract.mexc.com` is no longer used for private REST by default.


## v0059 notes
- `VERSION` updated to `0061 MEXC POSITION SYNC`.
- `LIMIT_TIMEOUT_SEC` default increased from 30 to 300 seconds.
- MEXC code `8950` / closing-only symbols are treated as non-retryable: no position slot is occupied, and the symbol is locked for `MEXC_RESTRICTED_SYMBOL_LOCK_SEC` seconds.
- MEXC private futures REST remains on `https://api.mexc.com`; WebSocket still uses `wss://contract.mexc.com/edge`.


## v0061 MEXC POSITION SYNC
- Default MEXC leverage is now 5x isolated.
- Default risk is now 1% per trade.
- Telegram open-position messages now show coin qty, USDT notional, leverage, margin mode, and estimated margin.
- Position list now shows notional, leverage, and estimated margin.
- Position lifecycle events now include PnL for TP/SL/time-stop closes when available.
- Strategy logic, scanner, and WebSocket logic were not changed.


## v0067 SAFE LOCAL STATE + HIDDEN MARGIN
- Raw MEXC futures state sync is now exchange-first.
- `/positions` reads native MEXC open positions and does not rely on ccxt position parsing.
- Open orders include normal orders plus plan/stop/TP-SL order scans when available.
- `/cancel_all`, `/close_all`, and Panic attempt raw exchange cleanup even if local bot state is empty.
- Balance output includes used/position/frozen margin diagnostics when MEXC returns them.

# v0064: cap margin per position as total_balance / max_open_positions
MARGIN_ALLOCATION_ENABLED=true


## v0067 SAFE LOCAL STATE + HIDDEN MARGIN
- Если MEXC не отдаёт строку позиции, бот не удаляет локальную позицию после входа.
- Если exchange TP/SL protection не поставился, бот оставляет позицию под локальным мониторингом TP/SL/time-stop вместо немедленного удаления состояния.
- /positions показывает protection mode и warning.
- /cancel_all и /close_all также чистят локальное состояние после успешной команды.


## v0125 TIMEZONE + REJECT DEBUG

- Scanner status time now uses fixed UTC+3 display time to match Telegram/Moscow time instead of server UTC.
- Main loop no longer overwrites detailed scanner/signal rejection reasons with the generic `no candidates passed signal engine` text when a real reason already exists.
- Version updated to `0125 TIMEZONE + REJECT DEBUG`.


## v0127 AI SCALPING COMPACT FEATURES

- Version updated to `0127 AI SCALPING COMPACT FEATURES`.
- BTC/ETH AI scalping no longer opens only because AI returned LONG/SHORT.
- Added local sweep/reclaim/range-edge setup gate before candidate creation.
- Added quality score for AI scalping setup; default minimum is `AI_SCALPING_SETUP_MIN_QUALITY_SCORE=58`.
- Added adaptive TP/SL for AI scalping from structure and ATR; old fixed BTC/ETH TP/SL values remain fallback only.
- Did not add consecutive-loss protection in this version.


## v0128 AI SCALPING LOCAL NORMAL MODE

- Version updated to `0128 AI SCALPING LOCAL NORMAL MODE`.
- For ETH/BTC AI scalping with `ai_scalping_quality_filters_enabled = false`, OpenAI approval is no longer required.
- Normal mode now trades from the local liquidity setup gate only: sweep, reclaim, range edge, spread, quality score, adaptive TP/SL.
- With `ai_scalping_quality_filters_enabled = true`, OpenAI remains mandatory as final validator.
- No loss-streak protection was added.


## v0129 AI SCALPING SAFE PROTECTION

- Version updated to `0129 AI SCALPING SAFE PROTECTION`.
- AI scalping no longer force-closes a live position when MEXC TP/SL confirmation is delayed or unavailable.
- After entry, AI scalping waits longer before placing TP/SL (`AI_SCALPING_PROTECTION_DELAY_SEC=3.0`) and retries protection more times.
- If exchange TP/SL is still not confirmed, the position remains under local monitoring: the bot closes on local `take_price` or `stop_price`, while the watchdog keeps trying to restore exchange protection.
- A successful native MEXC TPSL response with an id is treated as protected even if open-order visibility lags.


## v0156 FULL MEXC DEBUG LOGS
- Version updated to `0156 FULL MEXC DEBUG LOGS`.
- BTC spot orderbook imbalance threshold softened to `1.35`.
- ETH spot orderbook imbalance threshold softened to `1.30`.
- Old persisted generic `1.80` is treated as the old packaged default and no longer blocks BTC/ETH micro-scalp.


## v0156 FULL MEXC DEBUG LOGS
- MEXC TP/SL now uses clean entry first, then real exchange protection after positionId is visible.
- Fixed planorder triggerType: TP/SL direction is now sent in triggerType, not trend.
- Added executeCycle/reduceOnly/priceProtect fields to trigger-market plan orders.
- Entry-attached TP/SL is disabled by default via MEXC_ATTACH_TPSL_ON_ENTRY=false.


## v0161 MEXC DIRECT NATIVE TPSL FIX
- MEXC protection now places explicit trigger-market TP and SL plan orders first.
- Native by-position TP/SL is fallback, not the hidden first path.
- `/log` now filters out huge balance snapshots and shows TP/SL/order/protection payloads.
- Version updated to `0159 MEXC NATIVE TPSL FIRST FIX`.


## v0161 MEXC DIRECT NATIVE TPSL FIX
- Native `/stoporder/place` now uses the already confirmed live position row.
- Prevents TP/SL protection from closing before an actual native TP/SL POST is attempted.
- `/log` includes `mexc_stoporder_place_body` and longer output.


## v0161 MEXC DIRECT NATIVE TPSL FIX

- Places MEXC native TP/SL immediately after the live position row is found.
- Uses the same live position row with positionId/holdVol, avoiding the generic protection rediscovery race.
- /log must now show POST /api/v1/private/stoporder/place for every protected normal scalp entry.
- If direct native TP/SL fails, the old generic retry/fallback path still runs and the position is closed if protection is missing.


## v0170 BOOST ZERO FEE SCANNER
- Compared against the working Railway/Ollama bot.
- Native MEXC TP/SL now uses `/api/v1/private/stoporder/place` by `positionId` with `volType=2`, `profitLossVolType=SAME`, market TP/SL types, `takeProfitReverse=2`, `stopLossReverse=2`.
- Removed zero `takeProfitOrderPrice` / `stopLossOrderPrice` fields from native market TP/SL payload.
- Direct native TP/SL placement no longer depends on `strategy == ai_scalping`; any normal protected BTC/ETH scalp with TP+SL uses it.


## v0170 BOOST ZERO FEE SCANNER
- Fixed native TP/SL direct path calling non-existent `_price_to_precision`.
- Native `/stoporder/place` now uses existing `_mexc_price_to_precision`, so TP/SL can actually be posted after entry.


## v0170 BOOST ZERO FEE SCANNER
- Native MEXC `/stoporder/place` rows with `state=1`, `isFinished=0`, `errorCode=0`, `takeProfitPrice` and `stopLossPrice` are now treated as confirmed exchange TP/SL.
- Fixes false `LOCAL PROTECTION MODE` / `MISSING` messages when MEXC has already accepted position TP/SL.


## v0170 BOOST ZERO FEE SCANNER
- Fixed false LOCAL PROTECTION warnings for active MEXC native stoporder rows.
- Active `state=1`, `isFinished=0`, `errorCode=0` rows with TP/SL prices now count as confirmed exchange protection.
- Watchdog notifications now use `tp_exists/sl_exists` aliases correctly.


## v0181 BOOST LIVE TPSL SAFE EXECUTION

- One live Telegram BOOST panel is edited instead of spamming messages.
- Inline buttons: Rotation ON/OFF, Live panel ON/OFF, Refresh.
- While a BOOST trade is open, the bot keeps scanning in the background.
- If the open trade is already in profit and a stronger 0-fee impulse appears, the bot closes the current trade and rotates to the stronger coin on the next cycle.
- Defaults: rotation only if PnL >= 0.04%, strength multiplier 1.35x, score gap 5.0, cooldown 20s.

Commands: `/boost_start`, `/boost_stop`, `/boost_status`, `/boost_rotation`.


## v0181 BOOST LIVE TPSL SAFE EXECUTION

- `/boost_list BTC,ETH,SOL` sets trusted 0-fee BOOST whitelist. Lower/upper case and comma-separated lists are accepted. Tokens are normalized to `BTCUSDT`, `ETHUSDT`, etc.
- `/boost_list` shows the current whitelist.
- `/boost_list_del` clears all whitelist symbols and disables manual fee fallback.
