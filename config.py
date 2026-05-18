import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

VERSION = os.getenv("BOT_VERSION", "0049 ALL SPOT + ADAPTIVE UNIVERSE FIX")

def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default

@dataclass
class Defaults:
    live_trading: bool = env_bool("LIVE_TRADING", False)
    universe_mode: str = os.getenv("UNIVERSE_MODE", "adaptive")
    scan_market_source: str = os.getenv("SCAN_MARKET_SOURCE", "mexc_binance")
    max_symbols: int = env_int("MAX_SYMBOLS", 100)
    scan_interval_sec: int = env_int("SCAN_INTERVAL_SEC", 3)
    scanner_concurrency: int = env_int("SCANNER_CONCURRENCY", 5)
    scanner_error_slowdown_threshold: int = env_int("SCANNER_ERROR_SLOWDOWN_THRESHOLD", 5)
    scanner_slowdown_max_sec: int = env_int("SCANNER_SLOWDOWN_MAX_SEC", 15)
    ws_update_throttle_ms: int = env_int("WS_UPDATE_THROTTLE_MS", 500)
    ws_max_updates_per_batch: int = env_int("WS_MAX_UPDATES_PER_BATCH", 1000)
    ws_queue_limit: int = env_int("WS_QUEUE_LIMIT", 2000)
    ws_adaptive_slowdown_threshold: int = env_int("WS_ADAPTIVE_SLOWDOWN_THRESHOLD", 1000)
    symbol_refresh_sec: int = env_int("SYMBOL_REFRESH_SEC", 300)
    max_open_positions: int = env_int("MAX_OPEN_POSITIONS", 5)
    risk_pct: float = env_float("RISK_PCT", 0.005)
    strategy_mode: str = os.getenv("STRATEGY_MODE", "hybrid")
    auto_strategy_adaptation: bool = env_bool("AUTO_STRATEGY_ADAPTATION", True)
    regime_adaptation: bool = env_bool("REGIME_ADAPTATION", True)
    mirror_mode: str = os.getenv("MIRROR_MODE", "off")
    spot_confirmation_enabled: bool = env_bool("SPOT_CONFIRMATION_ENABLED", True)
    session_filter_enabled: bool = env_bool("SESSION_FILTER_ENABLED", True)
    america_short_bias_enabled: bool = env_bool("AMERICA_SHORT_BIAS_ENABLED", True)
    max_spread_pct: float = env_float("MAX_SPREAD_PCT", 0.20)
    max_slippage_pct: float = env_float("MAX_SLIPPAGE_PCT", 0.20)
    min_depth_usdt: float = env_float("MIN_DEPTH_USDT", 5000.0)
    max_daily_loss_pct: float = env_float("MAX_DAILY_LOSS_PCT", 3.0)
    max_consecutive_losses: int = env_int("MAX_CONSECUTIVE_LOSSES", 4)
    cooldown_after_close_sec: int = env_int("COOLDOWN_AFTER_CLOSE_SEC", 120)
    limit_timeout_sec: int = env_int("LIMIT_TIMEOUT_SEC", 30)
    proxy_enabled: bool = env_bool("PROXY_ENABLED", False)
    proxy_url: str = os.getenv("PROXY_URL", "")
    mexc_order_leverage: int = env_int("MEXC_ORDER_LEVERAGE", 1)
    mexc_order_open_type: int = env_int("MEXC_ORDER_OPEN_TYPE", 1)
    mexc_recv_window: int = env_int("MEXC_RECV_WINDOW", 20000)

DEFAULTS = Defaults()
DB_PATH = os.getenv("DATABASE_PATH", "bot_data.sqlite3")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_IDS = os.getenv("ADMIN_IDS", "")
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "mexc").lower()
