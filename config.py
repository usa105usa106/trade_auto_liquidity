import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Previous packaged version markers for regression tests: 0078 SCALP EXIT SAFETY | 0089 OPENAI PROMPT QUALITY FIX | 0096 SAFE STOP MARKET RECOVERY

# Previous packaged version marker kept for regression tests: 0078 SCALP EXIT SAFETY
# Previous packaged version marker kept for regression tests: 0092 RUN IMMEDIATE SCAN WAKEUP
# Previous packaged version marker kept for regression tests: 0155 REAL MEXC TPSL TRIGGER FIX
VERSION = os.getenv("BOT_VERSION", "0164 MEXC TPSL CONFIRM FIX")

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
    auto_close_on_protection_failed: bool = env_bool("AUTO_CLOSE_ON_PROTECTION_FAILED", True)

    protection_post_open_delay_sec: float = env_float("PROTECTION_POST_OPEN_DELAY_SEC", 1.5)
    protection_position_wait_sec: float = env_float("PROTECTION_POSITION_WAIT_SEC", 6.0)
    protection_position_poll_sec: float = env_float("PROTECTION_POSITION_POLL_SEC", 0.5)
    protection_min_trigger_pct: float = env_float("PROTECTION_MIN_TRIGGER_PCT", 0.12)
    protection_min_trigger_ticks: int = env_int("PROTECTION_MIN_TRIGGER_TICKS", 5)
    protection_distance_expand_mult: float = env_float("PROTECTION_DISTANCE_EXPAND_MULT", 1.25)
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
    liquidity_retest_quality_mode: str = os.getenv("LIQUIDITY_RETEST_QUALITY_MODE", "a_plus")
    scanner_reject_log_enabled: bool = env_bool("SCANNER_REJECT_LOG_ENABLED", True)
    liquidity_runner_enabled: bool = env_bool("LIQUIDITY_RUNNER_ENABLED", False)

    ai_scalping_symbols: str = os.getenv("AI_SCALPING_SYMBOLS", "BTC_USDT,ETH_USDT")
    ai_scalping_min_confidence: float = env_float("AI_SCALPING_MIN_CONFIDENCE", 0.52)
    ai_scalping_ai_entry_filter_enabled: bool = env_bool("AI_SCALPING_AI_ENTRY_FILTER_ENABLED", True)
    ai_scalping_tp_pct: float = env_float("AI_SCALPING_TP_PCT", 0.10)
    ai_scalping_sl_pct: float = env_float("AI_SCALPING_SL_PCT", 0.22)
    ai_scalping_btc_tp_pct: float = env_float("AI_SCALPING_BTC_TP_PCT", 0.09)
    ai_scalping_btc_sl_pct: float = env_float("AI_SCALPING_BTC_SL_PCT", 0.18)
    ai_scalping_eth_tp_pct: float = env_float("AI_SCALPING_ETH_TP_PCT", 0.12)
    ai_scalping_eth_sl_pct: float = env_float("AI_SCALPING_ETH_SL_PCT", 0.24)
    ai_scalping_btc_min_tp_pct: float = env_float("AI_SCALPING_BTC_MIN_TP_PCT", 0.08)
    ai_scalping_btc_max_tp_pct: float = env_float("AI_SCALPING_BTC_MAX_TP_PCT", 0.12)
    ai_scalping_eth_min_tp_pct: float = env_float("AI_SCALPING_ETH_MIN_TP_PCT", 0.10)
    ai_scalping_eth_max_tp_pct: float = env_float("AI_SCALPING_ETH_MAX_TP_PCT", 0.16)
    ai_scalping_sl_tp_multiplier: float = env_float("AI_SCALPING_SL_TP_MULTIPLIER", 2.0)
    ai_scalping_max_spread_pct: float = env_float("AI_SCALPING_MAX_SPREAD_PCT", 0.08)
    ai_scalping_spot_imbalance_ratio: float = env_float("AI_SCALPING_SPOT_IMBALANCE_RATIO", 1.35)
    ai_scalping_btc_spot_imbalance_ratio: float = env_float("AI_SCALPING_BTC_SPOT_IMBALANCE_RATIO", 1.35)
    ai_scalping_eth_spot_imbalance_ratio: float = env_float("AI_SCALPING_ETH_SPOT_IMBALANCE_RATIO", 1.30)
    ai_scalping_futures_momentum_min_pct: float = env_float("AI_SCALPING_FUTURES_MOMENTUM_MIN_PCT", 0.015)
    ai_scalping_futures_max_against_pct: float = env_float("AI_SCALPING_FUTURES_MAX_AGAINST_PCT", 0.035)
    ai_scalping_quality_filters_enabled: bool = env_bool("AI_SCALPING_QUALITY_FILTERS_ENABLED", False)
    ai_scalping_quality_min_confidence: float = env_float("AI_SCALPING_QUALITY_MIN_CONFIDENCE", 0.72)
    ai_scalping_quality_cooldown_sec: int = env_int("AI_SCALPING_QUALITY_COOLDOWN_SEC", 45)
    ai_scalping_quality_min_atr_pct: float = env_float("AI_SCALPING_QUALITY_MIN_ATR_PCT", 0.035)
    ai_scalping_quality_min_ema_gap_pct: float = env_float("AI_SCALPING_QUALITY_MIN_EMA_GAP_PCT", 0.015)
    ai_scalping_quality_min_ret_5m_abs_pct: float = env_float("AI_SCALPING_QUALITY_MIN_RET_5M_ABS_PCT", 0.035)
    ai_scalping_setup_min_quality_score: float = env_float("AI_SCALPING_SETUP_MIN_QUALITY_SCORE", 42.0)
    ai_scalping_ai_cooldown_sec: int = env_int("AI_SCALPING_AI_COOLDOWN_SEC", 8)
    ai_scalping_openai_fallback_enabled: bool = env_bool("AI_SCALPING_OPENAI_FALLBACK_ENABLED", False)
    ai_scalping_json_mode_enabled: bool = env_bool("AI_SCALPING_JSON_MODE_ENABLED", True)
    ai_scalping_liquidation_stop_mode: bool = env_bool("AI_SCALPING_LIQUIDATION_STOP_MODE", False)
    ai_scalping_liq_margin_pct: float = env_float("AI_SCALPING_LIQ_MARGIN_PCT", 0.01)
    ai_scalping_liq_buffer_pct: float = env_float("AI_SCALPING_LIQ_BUFFER_PCT", 0.00)
    ai_scalping_liq_max_leverage: int = env_int("AI_SCALPING_LIQ_MAX_LEVERAGE", 200)
    ai_scalping_protection_delay_sec: float = env_float("AI_SCALPING_PROTECTION_DELAY_SEC", 3.0)
    trade_margin_pct: float = env_float("TRADE_MARGIN_PCT", 0.10)

DEFAULTS = Defaults()
DB_PATH = os.getenv("DATABASE_PATH", "bot_data.sqlite3")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_IDS = os.getenv("ADMIN_IDS", "")
DEFAULT_EXCHANGE = os.getenv("DEFAULT_EXCHANGE", "mexc").lower()
