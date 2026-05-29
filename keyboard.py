from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton

MAIN_MENU = ReplyKeyboardMarkup([
    ["▶️ Run", "⏹ Stop"],
    ["📊 Status", "🚨 Panic"],
    ["📈 Positions", "📉 Stats"],
    ["🧯 Close All", "🧹 Cancel All"],
    ["💰 Balance", "🏓 Ping"],
    ["⚙️ Settings", "🔐 API"],
    ["🤖 AI BTC/ETH scalping"],
    ["₿ BTC AI 4H автопилот", "📊 BTC Status"],
    ["🧽 Clean BTC Orders", "🧪 BTC Backtest 4H"],
    ["🧪 BTC Backtest 1H", "🧪 Round Levels"],
    ["🧪 Strategy Lab", "🧪 Strategy Lab Extra"],
    ["📊 BTC Status"],
    ["⚡ быстрый отскок"],
    ["🔻 импульсный слив"],
    ["📊 orderflow impulse"],
    ["🌊 cascade hunter"],
    ["💪 strongest coin"],
    ["🗡 knife reversal"],
    ["🧠 multi strategy"],
    ["🚀 BOOST MODE", "🛑 STOP BOOST"],
    ["📊 BOOST STATUS", "📊 AI Stats"],
    ["⚙️ MEXC"],
], resize_keyboard=True)

def _onoff(settings: dict | None, key: str) -> str:
    return "✅ ON" if bool((settings or {}).get(key, False)) else "○ OFF"

def _value(settings: dict | None, key: str, default: str = "") -> str:
    val = (settings or {}).get(key, default)
    return str(val)

def format_duration_seconds(value, default: int = 5) -> str:
    try:
        sec = int(float(value))
    except Exception:
        sec = int(default)
    if sec >= 3600 and sec % 3600 == 0:
        hours = sec // 3600
        return f"{hours}h"
    if sec >= 60 and sec % 60 == 0:
        minutes = sec // 60
        return f"{minutes}m"
    return f"{sec}s"

def settings_menu(revision: int, settings: dict | None = None):
    r = str(revision)
    api_ready = bool((settings or {}).get("mexc_api_key") and (settings or {}).get("mexc_api_secret"))
    api_label = "✅ API saved" if api_ready else "○ API missing"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🌐 Universe: {_value(settings, 'universe_mode', 'adaptive')}", callback_data=f"menu:universe:{r}")],
        [InlineKeyboardButton(f"📡 Фьючи|Спот: {_value(settings, 'scan_market_source', 'mexc_binance')}", callback_data=f"menu:marketsource:{r}")],
        [InlineKeyboardButton(f"📈 Strategy: {_value(settings, 'strategy_mode', 'hybrid')}", callback_data=f"menu:strategy:{r}")],
        [InlineKeyboardButton(f"💧 Liq retest quality: {_value(settings, 'liquidity_retest_quality_mode', 'a_plus')}", callback_data=f"menu:liquidity_quality:{r}")],
        [InlineKeyboardButton(f"⏱ Scan: {format_duration_seconds((settings or {}).get('scan_interval_sec', 5))}", callback_data=f"menu:scan:{r}"), InlineKeyboardButton(f"🧵 Concurrency: {_value(settings, 'scanner_concurrency', '5')}", callback_data=f"menu:concurrency:{r}")],
        [InlineKeyboardButton(f"🔄 Refresh: {_value(settings, 'symbol_refresh_sec', '300')}s", callback_data=f"menu:refresh:{r}")],
        [InlineKeyboardButton(f"📊 Risk: {float((settings or {}).get('risk_pct', 0.005))*100:.2f}%", callback_data=f"menu:risk:{r}"), InlineKeyboardButton(f"🔥 Max Pos: {_value(settings, 'max_open_positions', '5')}", callback_data=f"menu:maxpos:{r}")],
        [InlineKeyboardButton(f"⚡ Live: {_onoff(settings, 'live_trading')}", callback_data=f"toggle:live_trading:{r}"), InlineKeyboardButton(api_label, callback_data=f"menu:api:{r}")],
        [InlineKeyboardButton(f"🧠 Auto Strategy: {_onoff(settings, 'auto_strategy_adaptation')}", callback_data=f"toggle:auto_strategy_adaptation:{r}")],
        [InlineKeyboardButton(f"🤖 ИИ анализ: {_onoff(settings, 'openai_analysis_enabled')} | {_value(settings, 'openai_model', 'gpt-5.4-mini')}", callback_data=f"menu:openai:{r}")],
        [InlineKeyboardButton(f"🛡 AI scalp quality: {_onoff(settings, 'ai_scalping_quality_filters_enabled')}", callback_data=f"toggle:ai_scalping_quality_filters_enabled:{r}")],
        [InlineKeyboardButton(f"🚀 Boost rotation: {_onoff(settings, 'boost_parallel_scan_enabled')}", callback_data=f"toggle:boost_parallel_scan_enabled:{r}")],
        [InlineKeyboardButton(f"⚠️ AI liq stop: {_onoff(settings, 'ai_scalping_liquidation_stop_mode')}", callback_data=f"toggle:ai_scalping_liquidation_stop_mode:{r}")],
        [InlineKeyboardButton(f"📊 Графики сделок: {_onoff(settings, 'trade_charts_enabled')}", callback_data=f"toggle:trade_charts_enabled:{r}")],
        [InlineKeyboardButton(f"🏃 Liquidity Runner: {_onoff(settings, 'liquidity_runner_enabled')}", callback_data=f"toggle:liquidity_runner_enabled:{r}")],
        [InlineKeyboardButton(f"🧭 Regime: {_onoff(settings, 'regime_adaptation')}", callback_data=f"toggle:regime_adaptation:{r}")],
        [InlineKeyboardButton(f"🪞 Mirror: {_value(settings, 'mirror_mode', 'off')}", callback_data=f"menu:mirror:{r}"), InlineKeyboardButton(f"🔎 Spot: {_onoff(settings, 'spot_confirmation_enabled')}", callback_data=f"toggle:spot_confirmation_enabled:{r}")],
        [InlineKeyboardButton(f"🌍 Session: {_onoff(settings, 'session_filter_enabled')}", callback_data=f"toggle:session_filter_enabled:{r}"), InlineKeyboardButton(f"🇺🇸 Bias: {_onoff(settings, 'america_short_bias_enabled')}", callback_data=f"toggle:america_short_bias_enabled:{r}")],
        [InlineKeyboardButton(f"🔌 WebSocket: {_onoff(settings, 'ws_enabled')}", callback_data=f"toggle:ws_enabled:{r}"), InlineKeyboardButton(f"🌊 WS: {_value(settings, 'ws_update_throttle_ms', '500')}ms", callback_data=f"menu:wsthrottle:{r}")],
        [InlineKeyboardButton(f"⚙️ MEXC lev {_value(settings, 'mexc_order_leverage', '5')}x | open {_value(settings, 'mexc_order_open_type', '1')} | rw {_value(settings, 'mexc_recv_window', '20000')}", callback_data=f"noop:mexc:{r}")],
    ])

def choices_menu(name: str, choices: list[tuple[str, str]], revision: int, current=None):
    rows = []
    current_s = str(current)
    for label, value in choices:
        selected = current_s == str(value)
        mark = "✅" if selected else "○"
        # Telegram clients can render leading symbols inconsistently; show selection both before and after label.
        text = f"{mark} {label}  • ВЫБРАНО" if selected else f"{mark} {label}"
        rows.append([InlineKeyboardButton(text, callback_data=f"set:{name}:{value}:{revision}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"menu:settings:{revision}")])
    return InlineKeyboardMarkup(rows)

def api_menu(revision: int, settings: dict | None = None):
    api_ready = bool((settings or {}).get("mexc_api_key") and (settings or {}).get("mexc_api_secret"))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ API saved" if api_ready else "○ API missing", callback_data=f"noop:api:{revision}")],
        [InlineKeyboardButton("🧪 Test API", callback_data=f"api:test:{revision}"), InlineKeyboardButton("🗑 Clear API", callback_data=f"api:clear:{revision}")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"menu:settings:{revision}")],
    ])

def openai_menu(revision: int, settings: dict | None = None):
    r = str(revision)
    key_ready = bool((settings or {}).get("openai_api_key"))
    env_fallback = bool((settings or {}).get("openai_env_fallback", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🤖 Check: {_onoff(settings, 'openai_analysis_enabled')}", callback_data=f"toggle:openai_analysis_enabled:{r}")],
        [InlineKeyboardButton(f"💬 Show AI decisions: {_onoff(settings, 'openai_show_decisions')}", callback_data=f"toggle:openai_show_decisions:{r}")],
        [InlineKeyboardButton(f"🧠 Model: {_value(settings, 'openai_model', 'gpt-5.4-mini')}", callback_data=f"menu:openai_model:{r}")],
        [InlineKeyboardButton(f"🛡 Mode: {_value(settings, 'openai_check_strength', 'medium')}", callback_data=f"menu:openai_strength:{r}")],
        [InlineKeyboardButton("✅ API key saved" if key_ready else ("ENV fallback ON" if env_fallback else "○ API key missing"), callback_data=f"openai:key_help:{r}")],
        [InlineKeyboardButton(f"🌐 Env fallback: {_onoff(settings, 'openai_env_fallback')}", callback_data=f"toggle:openai_env_fallback:{r}"), InlineKeyboardButton("🗑 Clear key", callback_data=f"openai:clear:{r}")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"menu:settings:{r}")],
    ])


def ai_stats_menu(revision: int):
    r = str(revision)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Current session stats", callback_data=f"aistats:current:{r}")],
        [InlineKeyboardButton("📊 Lifetime stats", callback_data=f"aistats:lifetime:{r}")],
        [InlineKeyboardButton("♻ Reset AI session", callback_data=f"aistats:reset:{r}")],
    ])
