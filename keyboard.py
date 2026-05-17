from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton

MAIN_MENU = ReplyKeyboardMarkup([
    ["▶️ Run", "⏹ Stop"],
    ["📊 Status", "🚨 Panic"],
    ["📈 Positions", "📉 Stats"],
    ["💰 Balance", "🏓 Ping"],
    ["⚙️ Settings", "🔐 API"],
], resize_keyboard=True)

def _onoff(settings: dict | None, key: str) -> str:
    return "✅ ON" if bool((settings or {}).get(key, False)) else "○ OFF"

def _value(settings: dict | None, key: str, default: str = "") -> str:
    val = (settings or {}).get(key, default)
    return str(val)

def settings_menu(revision: int, settings: dict | None = None):
    r = str(revision)
    api_ready = bool((settings or {}).get("mexc_api_key") and (settings or {}).get("mexc_api_secret"))
    api_label = "✅ API saved" if api_ready else "○ API missing"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🌐 Universe: {_value(settings, 'universe_mode', 'adaptive')}", callback_data=f"menu:universe:{r}")],
        [InlineKeyboardButton(f"📈 Strategy: {_value(settings, 'strategy_mode', 'hybrid')}", callback_data=f"menu:strategy:{r}")],
        [InlineKeyboardButton(f"⏱ Scan: {_value(settings, 'scan_interval_sec', '3')}s", callback_data=f"menu:scan:{r}"), InlineKeyboardButton(f"🔄 Refresh: {_value(settings, 'symbol_refresh_sec', '300')}s", callback_data=f"menu:refresh:{r}")],
        [InlineKeyboardButton(f"📊 Risk: {float((settings or {}).get('risk_pct', 0.005))*100:.2f}%", callback_data=f"menu:risk:{r}"), InlineKeyboardButton(f"🔥 Max Pos: {_value(settings, 'max_open_positions', '5')}", callback_data=f"menu:maxpos:{r}")],
        [InlineKeyboardButton(f"⚡ Live: {_onoff(settings, 'live_trading')}", callback_data=f"toggle:live_trading:{r}"), InlineKeyboardButton(api_label, callback_data=f"menu:api:{r}")],
        [InlineKeyboardButton(f"🧠 Auto Strategy: {_onoff(settings, 'auto_strategy_adaptation')}", callback_data=f"toggle:auto_strategy_adaptation:{r}")],
        [InlineKeyboardButton(f"🧭 Regime: {_onoff(settings, 'regime_adaptation')}", callback_data=f"toggle:regime_adaptation:{r}")],
        [InlineKeyboardButton(f"🪞 Mirror: {_value(settings, 'mirror_mode', 'off')}", callback_data=f"menu:mirror:{r}"), InlineKeyboardButton(f"🔎 Spot: {_onoff(settings, 'spot_confirmation_enabled')}", callback_data=f"toggle:spot_confirmation_enabled:{r}")],
        [InlineKeyboardButton(f"🌍 Session: {_onoff(settings, 'session_filter_enabled')}", callback_data=f"toggle:session_filter_enabled:{r}"), InlineKeyboardButton(f"🇺🇸 Bias: {_onoff(settings, 'america_short_bias_enabled')}", callback_data=f"toggle:america_short_bias_enabled:{r}")],
        [InlineKeyboardButton(f"🔌 WebSocket: {_onoff(settings, 'ws_enabled')}", callback_data=f"toggle:ws_enabled:{r}")],
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
