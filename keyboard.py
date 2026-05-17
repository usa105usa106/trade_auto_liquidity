from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton

MAIN_MENU = ReplyKeyboardMarkup([
    ["▶️ Run", "⏹ Stop"],
    ["📊 Status", "🚨 Panic"],
    ["📈 Positions", "📉 Stats"],
    ["💰 Balance", "🏓 Ping"],
    ["⚙️ Settings"],
], resize_keyboard=True)

def settings_menu(revision: int):
    r = str(revision)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Universe", callback_data=f"menu:universe:{r}"), InlineKeyboardButton("📈 Strategy", callback_data=f"menu:strategy:{r}")],
        [InlineKeyboardButton("⏱ Scan", callback_data=f"menu:scan:{r}"), InlineKeyboardButton("🔄 Refresh", callback_data=f"menu:refresh:{r}")],
        [InlineKeyboardButton("📊 Risk", callback_data=f"menu:risk:{r}"), InlineKeyboardButton("🔥 Max Pos", callback_data=f"menu:maxpos:{r}")],
        [InlineKeyboardButton("⚡ Live ON/OFF", callback_data=f"toggle:live_trading:{r}")],
        [InlineKeyboardButton("🧠 Auto Strategy", callback_data=f"toggle:auto_strategy_adaptation:{r}"), InlineKeyboardButton("🧭 Regime", callback_data=f"toggle:regime_adaptation:{r}")],
        [InlineKeyboardButton("🪞 Mirror", callback_data=f"menu:mirror:{r}"), InlineKeyboardButton("🔎 Spot Confirm", callback_data=f"toggle:spot_confirmation_enabled:{r}")],
        [InlineKeyboardButton("🌍 Session", callback_data=f"toggle:session_filter_enabled:{r}"), InlineKeyboardButton("🇺🇸 Short Bias", callback_data=f"toggle:america_short_bias_enabled:{r}")],
        [InlineKeyboardButton("🔌 WebSocket", callback_data=f"toggle:ws_enabled:{r}")],
    ])

def choices_menu(name: str, choices: list[tuple[str, str]], revision: int):
    rows = []
    for label, value in choices:
        rows.append([InlineKeyboardButton(label, callback_data=f"set:{name}:{value}:{revision}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"menu:settings:{revision}")])
    return InlineKeyboardMarkup(rows)
