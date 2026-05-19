import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Previous packaged version markers for regression tests: 0078 SCALP EXIT SAFETY | 0089 OPENAI PROMPT QUALITY FIX

# Previous packaged version marker kept for regression tests: 0078 SCALP EXIT SAFETY
VERSION = os.getenv("BOT_VERSION", "0091 OPENAI DECISION EDIT MODE")

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
    scan_interval_sec: int = env_int("SCAN_INTERVAL_SEC", 5)
    scanner_concurrency: int = env_int("SCANNER_CONCURRENCY", 5)
    scanner_error_slowdown_threshold: int = env_int("SCANNER_ERROR_SLOWDOWN_THRESHOLD", 5)
    scanner_slowdown_max_sec: int = env_int("SCANNER_SLOWDOWN_MAX_SEC", 15)
    ws_update_throttle_ms: int = env_int("WS_UPDATE_THROTTLE_MS", 500)
    ws_max_updates_per_batch: int = env_int("WS_MAX_UPDATES_PER_BATCH", 1000)
    ws_queue_limit: int = env_int("WS_QUEUE_LIMIT", 2000)
    ws_adaptive_slowdown_threshold: int = env_int("WS_ADAPTIVE_SLOWDOWN_THRESHOLD", 1000)
    symbol_refresh_sec: int = env_int("SYMBOL_REFRESH_SEC", 300)
    max_open_positions: int = env_int("MAX_OPEN_POSITIONS", 5)
    risk_pct: float = env_float("RISK_PCT", 0.01)
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
    limit_timeout_sec: int = env_int("LIMIT_TIMEOUT_SEC", 300)
    proxy_enabled: bool = env_bool("PROXY_ENABLED", False)
    proxy_url: str = os.getenv("PROXY_URL", "")
    mexc_order_leverage: int = env_int("MEXC_ORDER_LEVERAGE", 5)
    mexc_order_open_type: int = env_int("MEXC_ORDER_OPEN_TYPE", 1)
    mexc_recv_window: int = env_int("MEXC_RECV_WINDOW", 20000)
    margin_allocation_enabled: bool = env_bool("MARGIN_ALLOCATION_ENABLED", True)
    require_exchange_protection: bool = env_bool("REQUIRE_EXCHANGE_PROTECTION", True)
    auto_close_on_protection_failed: bool = env_bool("AUTO_CLOSE_ON_PROTECTION_FAILED", False)
    liquidity_retest_default_rr: float = env_float("LIQUIDITY_RETEST_DEFAULT_RR", 3.0)
    liquidity_retest_sl_buffer_pct: float = env_float("LIQUIDITY_RETEST_SL_BUFFER_PCT", 0.04)
    liquidity_retest_time_stop_sec: int = env_int("LIQUIDITY_RETEST_TIME_STOP_SEC", 1800)
    liquidity_retest_min_displacement_pct: float = env_float("LIQUIDITY_RETEST_MIN_DISPLACEMENT_PCT", 0.10)
    liquidity_retest_min_displacement_body: float = env_float("LIQUIDITY_RETEST_MIN_DISPLACEMENT_BODY", 0.55)
    liquidity_retest_min_volume_ratio: float = env_float("LIQUIDITY_RETEST_MIN_VOLUME_RATIO", 1.15)
    liquidity_retest_min_target_rr: float = env_float("LIQUIDITY_RETEST_MIN_TARGET_RR", 1.8)
    liquidity_retest_zone_tolerance_pct: float = env_float("LIQUIDITY_RETEST_ZONE_TOLERANCE_PCT", 0.08)
    liquidity_retest_min_sweep_wick: float = env_float("LIQUIDITY_RETEST_MIN_SWEEP_WICK", 0.25)
    liquidity_retest_min_reclaim_pct: float = env_float("LIQUIDITY_RETEST_MIN_RECLAIM_PCT", 0.04)
    liquidity_retest_max_spread_pct: float = env_float("LIQUIDITY_RETEST_MAX_SPREAD_PCT", 0.18)
    liquidity_retest_min_retest_rejection_wick: float = env_float("LIQUIDITY_RETEST_MIN_RETEST_REJECTION_WICK", 0.25)
    liquidity_retest_min_zone_quality: float = env_float("LIQUIDITY_RETEST_MIN_ZONE_QUALITY", 2.0)
    liquidity_retest_mtf_enabled: bool = env_bool("LIQUIDITY_RETEST_MTF_ENABLED", True)
    liquidity_retest_min_mtf_score: float = env_float("LIQUIDITY_RETEST_MIN_MTF_SCORE", -0.25)
    liquidity_retest_require_clean_path: bool = env_bool("LIQUIDITY_RETEST_REQUIRE_CLEAN_PATH", False)

DEFAULTS = Defaults()
DB_PATH = os.getenv("DATABASE_PATH", "bot_data.sqlite3")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_IDS = os.getenv("ADMIN_IDS", "")
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "mexc").lower()
