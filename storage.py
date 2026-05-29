from __future__ import annotations
import time
import json
import os
import aiosqlite
from typing import Any, Optional
from config import DB_PATH, DEFAULTS
from runtime_secrets import merge_secrets_into_settings, apply_secret_backup_to_env, save_secret_backup

DEFAULT_SETTINGS = {
    "live_trading": DEFAULTS.live_trading,
    "universe_mode": DEFAULTS.universe_mode,
    "max_symbols": DEFAULTS.max_symbols,
    "scan_interval_sec": DEFAULTS.scan_interval_sec,
    "scanner_concurrency": DEFAULTS.scanner_concurrency,
    "scanner_error_slowdown_threshold": DEFAULTS.scanner_error_slowdown_threshold,
    "scanner_slowdown_max_sec": DEFAULTS.scanner_slowdown_max_sec,
    "ws_update_throttle_ms": DEFAULTS.ws_update_throttle_ms,
    "ws_max_updates_per_batch": DEFAULTS.ws_max_updates_per_batch,
    "ws_queue_limit": DEFAULTS.ws_queue_limit,
    "ws_adaptive_slowdown_threshold": DEFAULTS.ws_adaptive_slowdown_threshold,
    "symbol_refresh_sec": DEFAULTS.symbol_refresh_sec,
    "max_open_positions": DEFAULTS.max_open_positions,
    "risk_pct": DEFAULTS.risk_pct,
    "strategy_mode": DEFAULTS.strategy_mode,
    "auto_strategy_adaptation": DEFAULTS.auto_strategy_adaptation,
    "regime_adaptation": DEFAULTS.regime_adaptation,
    "mirror_mode": DEFAULTS.mirror_mode,
    "spot_confirmation_enabled": DEFAULTS.spot_confirmation_enabled,
    "scan_market_source": DEFAULTS.scan_market_source,
    "session_filter_enabled": DEFAULTS.session_filter_enabled,
    "america_short_bias_enabled": DEFAULTS.america_short_bias_enabled,
    "max_spread_pct": DEFAULTS.max_spread_pct,
    "max_slippage_pct": DEFAULTS.max_slippage_pct,
    "min_depth_usdt": DEFAULTS.min_depth_usdt,
    "max_daily_loss_pct": DEFAULTS.max_daily_loss_pct,
    "max_consecutive_losses": DEFAULTS.max_consecutive_losses,
    "cooldown_after_close_sec": DEFAULTS.cooldown_after_close_sec,
    "limit_timeout_sec": DEFAULTS.limit_timeout_sec,
    "time_stop_sec": int(os.getenv("TIME_STOP_SEC", "300")),
    "breakeven_trigger_pct": float(os.getenv("BREAKEVEN_TRIGGER_PCT", "0.12")),
    "breakeven_offset_pct": float(os.getenv("BREAKEVEN_OFFSET_PCT", "0.01")),
    "scalp_exit_enabled": os.getenv("SCALP_EXIT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "scalp_trailing_enabled": os.getenv("SCALP_TRAILING_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "scalp_trailing_start_pct": float(os.getenv("SCALP_TRAILING_START_PCT", "0.18")),
    "scalp_trailing_giveback_pct": float(os.getenv("SCALP_TRAILING_GIVEBACK_PCT", "0.08")),
    "smart_time_stop_min_sec": int(os.getenv("SMART_TIME_STOP_MIN_SEC", "45")),
    "smart_time_stop_stale_abs_pct": float(os.getenv("SMART_TIME_STOP_STALE_ABS_PCT", "0.04")),
    "smart_time_stop_extend_profit_pct": float(os.getenv("SMART_TIME_STOP_EXTEND_PROFIT_PCT", "0.08")),
    "smart_time_stop_max_extend_sec": int(os.getenv("SMART_TIME_STOP_MAX_EXTEND_SEC", "180")),
    "require_exchange_protection": os.getenv("REQUIRE_EXCHANGE_PROTECTION", "true").lower() in {"1", "true", "yes", "on"},
    "auto_close_on_protection_failed": DEFAULTS.auto_close_on_protection_failed,
    "liquidity_retest_default_rr": DEFAULTS.liquidity_retest_default_rr,
    "liquidity_retest_sl_buffer_pct": DEFAULTS.liquidity_retest_sl_buffer_pct,
    "liquidity_retest_time_stop_sec": DEFAULTS.liquidity_retest_time_stop_sec,
    "liquidity_retest_min_displacement_pct": DEFAULTS.liquidity_retest_min_displacement_pct,
    "liquidity_retest_min_displacement_body": DEFAULTS.liquidity_retest_min_displacement_body,
    "liquidity_retest_min_volume_ratio": DEFAULTS.liquidity_retest_min_volume_ratio,
    "liquidity_retest_min_target_rr": DEFAULTS.liquidity_retest_min_target_rr,
    "liquidity_retest_zone_tolerance_pct": DEFAULTS.liquidity_retest_zone_tolerance_pct,
    "liquidity_retest_min_sweep_wick": DEFAULTS.liquidity_retest_min_sweep_wick,
    "liquidity_retest_min_reclaim_pct": DEFAULTS.liquidity_retest_min_reclaim_pct,
    "liquidity_retest_max_spread_pct": DEFAULTS.liquidity_retest_max_spread_pct,
    "liquidity_retest_min_retest_rejection_wick": DEFAULTS.liquidity_retest_min_retest_rejection_wick,
    "liquidity_retest_min_zone_quality": DEFAULTS.liquidity_retest_min_zone_quality,
    "liquidity_retest_mtf_enabled": DEFAULTS.liquidity_retest_mtf_enabled,
    "liquidity_retest_min_mtf_score": DEFAULTS.liquidity_retest_min_mtf_score,
    "liquidity_retest_require_clean_path": DEFAULTS.liquidity_retest_require_clean_path,
    "liquidity_retest_quality_mode": DEFAULTS.liquidity_retest_quality_mode,
    "scanner_reject_log_enabled": DEFAULTS.scanner_reject_log_enabled,
    "liquidity_runner_enabled": DEFAULTS.liquidity_runner_enabled,

    "ai_scalping_symbols": DEFAULTS.ai_scalping_symbols,
    "ai_scalping_min_confidence": DEFAULTS.ai_scalping_min_confidence,
    "ai_scalping_ai_entry_filter_enabled": DEFAULTS.ai_scalping_ai_entry_filter_enabled,
    "ai_scalping_tp_pct": DEFAULTS.ai_scalping_tp_pct,
    "ai_scalping_sl_pct": DEFAULTS.ai_scalping_sl_pct,
    "ai_scalping_btc_tp_pct": DEFAULTS.ai_scalping_btc_tp_pct,
    "ai_scalping_btc_sl_pct": DEFAULTS.ai_scalping_btc_sl_pct,
    "ai_scalping_eth_tp_pct": DEFAULTS.ai_scalping_eth_tp_pct,
    "ai_scalping_eth_sl_pct": DEFAULTS.ai_scalping_eth_sl_pct,
    "ai_scalping_btc_min_tp_pct": DEFAULTS.ai_scalping_btc_min_tp_pct,
    "ai_scalping_btc_max_tp_pct": DEFAULTS.ai_scalping_btc_max_tp_pct,
    "ai_scalping_eth_min_tp_pct": DEFAULTS.ai_scalping_eth_min_tp_pct,
    "ai_scalping_eth_max_tp_pct": DEFAULTS.ai_scalping_eth_max_tp_pct,
    "ai_scalping_sl_tp_multiplier": DEFAULTS.ai_scalping_sl_tp_multiplier,
    "ai_scalping_max_spread_pct": DEFAULTS.ai_scalping_max_spread_pct,
    "ai_scalping_spot_imbalance_ratio": DEFAULTS.ai_scalping_spot_imbalance_ratio,
    "ai_scalping_btc_spot_imbalance_ratio": DEFAULTS.ai_scalping_btc_spot_imbalance_ratio,
    "ai_scalping_eth_spot_imbalance_ratio": DEFAULTS.ai_scalping_eth_spot_imbalance_ratio,
    "ai_scalping_futures_momentum_min_pct": DEFAULTS.ai_scalping_futures_momentum_min_pct,
    "ai_scalping_futures_max_against_pct": DEFAULTS.ai_scalping_futures_max_against_pct,
    "ai_scalping_quality_filters_enabled": DEFAULTS.ai_scalping_quality_filters_enabled,
    "ai_scalping_quality_min_confidence": DEFAULTS.ai_scalping_quality_min_confidence,
    "ai_scalping_quality_cooldown_sec": DEFAULTS.ai_scalping_quality_cooldown_sec,
    "ai_scalping_quality_min_atr_pct": DEFAULTS.ai_scalping_quality_min_atr_pct,
    "ai_scalping_quality_min_ema_gap_pct": DEFAULTS.ai_scalping_quality_min_ema_gap_pct,
    "ai_scalping_quality_min_ret_5m_abs_pct": DEFAULTS.ai_scalping_quality_min_ret_5m_abs_pct,
    "ai_scalping_setup_min_quality_score": DEFAULTS.ai_scalping_setup_min_quality_score,
    "ai_scalping_ai_cooldown_sec": DEFAULTS.ai_scalping_ai_cooldown_sec,
    "ai_scalping_openai_fallback_enabled": DEFAULTS.ai_scalping_openai_fallback_enabled,
    "ai_scalping_json_mode_enabled": DEFAULTS.ai_scalping_json_mode_enabled,
    "ai_scalping_liquidation_stop_mode": DEFAULTS.ai_scalping_liquidation_stop_mode,
    "ai_scalping_liq_margin_pct": DEFAULTS.ai_scalping_liq_margin_pct,
    "ai_scalping_liq_buffer_pct": DEFAULTS.ai_scalping_liq_buffer_pct,
    "ai_scalping_liq_max_leverage": DEFAULTS.ai_scalping_liq_max_leverage,
    "trade_margin_pct": DEFAULTS.trade_margin_pct,
    "quick_bounce_enabled": DEFAULTS.quick_bounce_enabled,
    "quick_bounce_top_coins": DEFAULTS.quick_bounce_top_coins,
    "quick_bounce_scan_interval_sec": DEFAULTS.quick_bounce_scan_interval_sec,
    "quick_bounce_trade_margin_pct": DEFAULTS.quick_bounce_trade_margin_pct,
    "quick_bounce_max_open_positions": DEFAULTS.quick_bounce_max_open_positions,
    "quick_bounce_leverage": DEFAULTS.quick_bounce_leverage,
    "quick_bounce_tp_pct": DEFAULTS.quick_bounce_tp_pct,
    "quick_bounce_sl_pct": DEFAULTS.quick_bounce_sl_pct,
    "quick_bounce_rr": DEFAULTS.quick_bounce_rr,
    "quick_bounce_time_stop_sec": DEFAULTS.quick_bounce_time_stop_sec,
    "quick_bounce_drop_4h_pct": DEFAULTS.quick_bounce_drop_4h_pct,
    "quick_bounce_pump_4h_pct": DEFAULTS.quick_bounce_pump_4h_pct,
    "quick_bounce_reversal_pct": DEFAULTS.quick_bounce_reversal_pct,
    "quick_bounce_min_volume_ratio": DEFAULTS.quick_bounce_min_volume_ratio,
    "quick_bounce_max_spread_pct": DEFAULTS.quick_bounce_max_spread_pct,
    "quick_bounce_min_24h_volume_usdt": DEFAULTS.quick_bounce_min_24h_volume_usdt,
    "quick_bounce_btc_filter_enabled": DEFAULTS.quick_bounce_btc_filter_enabled,
    "quick_bounce_btc_max_drop_1h_pct": DEFAULTS.quick_bounce_btc_max_drop_1h_pct,
    "quick_bounce_btc_max_pump_1h_pct": DEFAULTS.quick_bounce_btc_max_pump_1h_pct,
    "quick_bounce_cooldown_after_close_sec": DEFAULTS.quick_bounce_cooldown_after_close_sec,
    "quick_bounce_max_daily_loss_pct": DEFAULTS.quick_bounce_max_daily_loss_pct,
    "quick_bounce_anomaly_timeframe": DEFAULTS.quick_bounce_anomaly_timeframe,
    "quick_bounce_confirm_timeframe": DEFAULTS.quick_bounce_confirm_timeframe,
    "quick_bounce_max_candidates": DEFAULTS.quick_bounce_max_candidates,

    "impulse_dump_enabled": DEFAULTS.impulse_dump_enabled,
    "impulse_dump_top_coins": DEFAULTS.impulse_dump_top_coins,
    "impulse_dump_scan_interval_sec": DEFAULTS.impulse_dump_scan_interval_sec,
    "impulse_dump_trade_margin_pct": DEFAULTS.impulse_dump_trade_margin_pct,
    "impulse_dump_max_open_positions": DEFAULTS.impulse_dump_max_open_positions,
    "impulse_dump_leverage": DEFAULTS.impulse_dump_leverage,
    "impulse_dump_sl_pct": DEFAULTS.impulse_dump_sl_pct,
    "impulse_dump_total_drop_target_pct": DEFAULTS.impulse_dump_total_drop_target_pct,
    "impulse_dump_min_drop_pct": DEFAULTS.impulse_dump_min_drop_pct,
    "impulse_dump_max_drop_pct": DEFAULTS.impulse_dump_max_drop_pct,
    "impulse_dump_15m_min_drop_pct": DEFAULTS.impulse_dump_15m_min_drop_pct,
    "impulse_dump_15m_max_drop_pct": DEFAULTS.impulse_dump_15m_max_drop_pct,
    "impulse_dump_4h_max_drop_pct": DEFAULTS.impulse_dump_4h_max_drop_pct,
    "impulse_dump_24h_max_drop_pct": DEFAULTS.impulse_dump_24h_max_drop_pct,
    "impulse_dump_time_stop_sec": DEFAULTS.impulse_dump_time_stop_sec,
    "impulse_dump_min_volume_ratio": DEFAULTS.impulse_dump_min_volume_ratio,
    "impulse_dump_max_spread_pct": DEFAULTS.impulse_dump_max_spread_pct,
    "impulse_dump_min_24h_volume_usdt": DEFAULTS.impulse_dump_min_24h_volume_usdt,
    "impulse_dump_btc_filter_enabled": DEFAULTS.impulse_dump_btc_filter_enabled,
    "impulse_dump_btc_max_pump_1h_pct": DEFAULTS.impulse_dump_btc_max_pump_1h_pct,
    "impulse_dump_cooldown_after_close_sec": DEFAULTS.impulse_dump_cooldown_after_close_sec,
    "impulse_dump_max_daily_loss_pct": DEFAULTS.impulse_dump_max_daily_loss_pct,
    "impulse_dump_stop_after_consecutive_sl": DEFAULTS.impulse_dump_stop_after_consecutive_sl,
    "impulse_dump_anomaly_timeframe": DEFAULTS.impulse_dump_anomaly_timeframe,
    "impulse_dump_confirm_timeframe": DEFAULTS.impulse_dump_confirm_timeframe,
    "impulse_dump_max_candidates": DEFAULTS.impulse_dump_max_candidates,
    "impulse_dump_manage_only_tpsl": DEFAULTS.impulse_dump_manage_only_tpsl,
    "orderflow_impulse_enabled": DEFAULTS.orderflow_impulse_enabled,
    "orderflow_impulse_top_coins": DEFAULTS.orderflow_impulse_top_coins,
    "orderflow_impulse_scan_interval_sec": DEFAULTS.orderflow_impulse_scan_interval_sec,
    "orderflow_impulse_trade_margin_pct": DEFAULTS.orderflow_impulse_trade_margin_pct,
    "orderflow_impulse_max_open_positions": DEFAULTS.orderflow_impulse_max_open_positions,
    "orderflow_impulse_leverage": DEFAULTS.orderflow_impulse_leverage,
    "orderflow_impulse_tp_pct": DEFAULTS.orderflow_impulse_tp_pct,
    "orderflow_impulse_sl_pct": DEFAULTS.orderflow_impulse_sl_pct,
    "orderflow_impulse_time_stop_sec": DEFAULTS.orderflow_impulse_time_stop_sec,
    "orderflow_impulse_min_volume_ratio": DEFAULTS.orderflow_impulse_min_volume_ratio,
    "orderflow_impulse_min_trend_pct": DEFAULTS.orderflow_impulse_min_trend_pct,
    "orderflow_impulse_min_imbalance_abs": DEFAULTS.orderflow_impulse_min_imbalance_abs,
    "orderflow_impulse_max_spread_pct": DEFAULTS.orderflow_impulse_max_spread_pct,
    "orderflow_impulse_min_24h_volume_usdt": DEFAULTS.orderflow_impulse_min_24h_volume_usdt,
    "orderflow_impulse_manage_only_tpsl": DEFAULTS.orderflow_impulse_manage_only_tpsl,

    "cascade_hunter_enabled": DEFAULTS.cascade_hunter_enabled,
    "cascade_hunter_top_coins": DEFAULTS.cascade_hunter_top_coins,
    "cascade_hunter_scan_interval_sec": DEFAULTS.cascade_hunter_scan_interval_sec,
    "cascade_hunter_trade_margin_pct": DEFAULTS.cascade_hunter_trade_margin_pct,
    "cascade_hunter_max_open_positions": DEFAULTS.cascade_hunter_max_open_positions,
    "cascade_hunter_leverage": DEFAULTS.cascade_hunter_leverage,
    "cascade_hunter_tp_pct": DEFAULTS.cascade_hunter_tp_pct,
    "cascade_hunter_sl_pct": DEFAULTS.cascade_hunter_sl_pct,
    "cascade_hunter_time_stop_sec": DEFAULTS.cascade_hunter_time_stop_sec,
    "cascade_hunter_min_liq_usd_1m": DEFAULTS.cascade_hunter_min_liq_usd_1m,
    "cascade_hunter_min_pressure_ratio": DEFAULTS.cascade_hunter_min_pressure_ratio,
    "cascade_hunter_min_volume_ratio": DEFAULTS.cascade_hunter_min_volume_ratio,
    "cascade_hunter_min_price_move_pct": DEFAULTS.cascade_hunter_min_price_move_pct,
    "cascade_hunter_max_spread_pct": DEFAULTS.cascade_hunter_max_spread_pct,
    "cascade_hunter_min_24h_volume_usdt": DEFAULTS.cascade_hunter_min_24h_volume_usdt,

    "strongest_coin_enabled": DEFAULTS.strongest_coin_enabled,
    "strongest_coin_top_coins": DEFAULTS.strongest_coin_top_coins,
    "strongest_coin_scan_interval_sec": DEFAULTS.strongest_coin_scan_interval_sec,
    "strongest_coin_trade_margin_pct": DEFAULTS.strongest_coin_trade_margin_pct,
    "strongest_coin_max_open_positions": DEFAULTS.strongest_coin_max_open_positions,
    "strongest_coin_leverage": DEFAULTS.strongest_coin_leverage,
    "strongest_coin_min_24h_volume_usdt": DEFAULTS.strongest_coin_min_24h_volume_usdt,
    "strongest_coin_max_spread_pct": DEFAULTS.strongest_coin_max_spread_pct,
    "strongest_coin_min_strength_score": DEFAULTS.strongest_coin_min_strength_score,
    "strongest_coin_min_rs_btc_15m_pct": DEFAULTS.strongest_coin_min_rs_btc_15m_pct,
    "strongest_coin_btc_panic_5m_pct": DEFAULTS.strongest_coin_btc_panic_5m_pct,
    "strongest_coin_min_pullback_pct": DEFAULTS.strongest_coin_min_pullback_pct,
    "strongest_coin_max_pullback_pct": DEFAULTS.strongest_coin_max_pullback_pct,
    "strongest_coin_max_pullback_depth": DEFAULTS.strongest_coin_max_pullback_depth,
    "strongest_coin_min_hold_recovery_pct": DEFAULTS.strongest_coin_min_hold_recovery_pct,
    "strongest_coin_stop_buffer_pct": DEFAULTS.strongest_coin_stop_buffer_pct,
    "strongest_coin_min_sl_pct": DEFAULTS.strongest_coin_min_sl_pct,
    "strongest_coin_max_sl_pct": DEFAULTS.strongest_coin_max_sl_pct,
    "strongest_coin_tp1_r": DEFAULTS.strongest_coin_tp1_r,
    "strongest_coin_tp2_r": DEFAULTS.strongest_coin_tp2_r,
    "strongest_coin_tp1_fraction": DEFAULTS.strongest_coin_tp1_fraction,
    "strongest_coin_time_stop_sec": DEFAULTS.strongest_coin_time_stop_sec,
    "strongest_coin_cooldown_after_close_sec": DEFAULTS.strongest_coin_cooldown_after_close_sec,
    "knife_reversal_enabled": DEFAULTS.knife_reversal_enabled,
    "knife_reversal_top_coins": DEFAULTS.knife_reversal_top_coins,
    "knife_reversal_scan_interval_sec": DEFAULTS.knife_reversal_scan_interval_sec,
    "knife_reversal_trade_margin_pct": DEFAULTS.knife_reversal_trade_margin_pct,
    "knife_reversal_max_open_positions": DEFAULTS.knife_reversal_max_open_positions,
    "knife_reversal_leverage": DEFAULTS.knife_reversal_leverage,
    "knife_reversal_tp_pct": DEFAULTS.knife_reversal_tp_pct,
    "knife_reversal_wick_sl_buffer_pct": DEFAULTS.knife_reversal_wick_sl_buffer_pct,
    "knife_reversal_min_24h_volume_usdt": DEFAULTS.knife_reversal_min_24h_volume_usdt,
    "knife_reversal_min_wick_pct": DEFAULTS.knife_reversal_min_wick_pct,
    "knife_reversal_min_reclaim_pct": DEFAULTS.knife_reversal_min_reclaim_pct,
    "knife_reversal_min_volume_ratio": DEFAULTS.knife_reversal_min_volume_ratio,
    "knife_reversal_min_delta_ratio": DEFAULTS.knife_reversal_min_delta_ratio,
    "knife_reversal_min_imbalance": DEFAULTS.knife_reversal_min_imbalance,
    "knife_reversal_max_spread_pct": DEFAULTS.knife_reversal_max_spread_pct,
    "knife_reversal_time_stop_sec": DEFAULTS.knife_reversal_time_stop_sec,
    "multi_strategy_enabled": DEFAULTS.multi_strategy_enabled,
    "multi_strategy_top_coins": DEFAULTS.multi_strategy_top_coins,
    "multi_strategy_scan_interval_sec": DEFAULTS.multi_strategy_scan_interval_sec,
    "multi_strategy_max_open_positions": DEFAULTS.multi_strategy_max_open_positions,
    "boost_zero_fee_scanner_enabled": DEFAULTS.boost_zero_fee_scanner_enabled,
    "boost_balance_share": DEFAULTS.boost_balance_share,
    "boost_trade_margin_pct": 0.35,
    "boost_use_full_bank_per_trade": False,
    "boost_live_slippage_buffer_pct": 0.018,
    "boost_spread_edge_mult": 1.6,
    "boost_tp_spread_mult": 2.2,
    "boost_tp_atr_mult": 0.70,
    # v0219: HUNTER fast profit extraction. These are PRICE-move pct,
    # not leveraged ROE. Exchange uPnL must still confirm real profit.
    "boost_fast_profit_enabled": True,
    "boost_fast_profit_min_pct": 0.11,
    "boost_fast_profit_exchange_min_pct": 0.09,
    "boost_fast_profit_min_age_sec": 2,
    "boost_fast_profit_max_hold_sec": 12,
    "boost_fast_trailing_start_pct": 0.022,
    "boost_fast_trailing_giveback_pct": 0.007,
    "boost_momentum_decay_profit_pct": 0.006,
    "boost_target_multiplier": DEFAULTS.boost_target_multiplier,
    "boost_session_hours": DEFAULTS.boost_session_hours,
    "boost_max_session_loss_pct": DEFAULTS.boost_max_session_loss_pct,
    "boost_max_consecutive_losses": DEFAULTS.boost_max_consecutive_losses,
    "boost_max_symbols_scan": DEFAULTS.boost_max_symbols_scan,
    "boost_min_checked_per_cycle": 40,
    "boost_max_checked_per_cycle": 100,
    "boost_min_quote_volume_usdt": DEFAULTS.boost_min_quote_volume_usdt,
    "boost_min_atr_pct": DEFAULTS.boost_min_atr_pct,
    "boost_max_spread_pct": DEFAULTS.boost_max_spread_pct,
    "boost_spot_imbalance_ratio": DEFAULTS.boost_spot_imbalance_ratio,
    "boost_futures_momentum_min_pct": 0.028,
    "boost_futures_max_against_pct": DEFAULTS.boost_futures_max_against_pct,
    "boost_min_tp_pct": DEFAULTS.boost_min_tp_pct,
    "boost_max_tp_pct": DEFAULTS.boost_max_tp_pct,
    "boost_sl_tp_multiplier": DEFAULTS.boost_sl_tp_multiplier,
    "boost_scan_interval_sec": DEFAULTS.boost_scan_interval_sec,
    "boost_allow_fee_fallback": DEFAULTS.boost_allow_fee_fallback,
    "boost_zero_fee_symbols": DEFAULTS.boost_zero_fee_symbols,
    "boost_live_panel_enabled": DEFAULTS.boost_live_panel_enabled,
    "boost_live_panel_interval_sec": DEFAULTS.boost_live_panel_interval_sec,
    "boost_parallel_scan_enabled": DEFAULTS.boost_parallel_scan_enabled,
    "boost_rotate_only_if_profit": DEFAULTS.boost_rotate_only_if_profit,
    "boost_min_profit_to_rotate_pct": DEFAULTS.boost_min_profit_to_rotate_pct,
    "boost_rotate_strength_multiplier": DEFAULTS.boost_rotate_strength_multiplier,
    "boost_rotate_min_score_gap": DEFAULTS.boost_rotate_min_score_gap,
    "boost_rotate_cooldown_sec": DEFAULTS.boost_rotate_cooldown_sec,
    "boost_rescue_rotation_enabled": DEFAULTS.boost_rescue_rotation_enabled,
    "boost_rescue_min_score_multiplier": DEFAULTS.boost_rescue_min_score_multiplier,
    "boost_rescue_min_score_gap": DEFAULTS.boost_rescue_min_score_gap,
    "boost_rescue_expected_move_loss_mult": DEFAULTS.boost_rescue_expected_move_loss_mult,
    "boost_rescue_max_loss_pct": DEFAULTS.boost_rescue_max_loss_pct,
    "boost_rescue_cooldown_sec": DEFAULTS.boost_rescue_cooldown_sec,
    "boost_rescue_max_per_hour": DEFAULTS.boost_rescue_max_per_hour,
    "boost_session_start_ts": 0.0,
    "boost_session_start_equity": 0.0,
    "boost_session_bank_usdt": 0.0,
    "boost_session_target_profit_usdt": 0.0,
    "boost_prev_scan_interval_sec": 0,
    "ai_scalping_protection_delay_sec": DEFAULTS.ai_scalping_protection_delay_sec,
    "proxy_enabled": DEFAULTS.proxy_enabled,
    "proxy_url": DEFAULTS.proxy_url,
    "mexc_order_leverage": DEFAULTS.mexc_order_leverage,
    "mexc_order_open_type": DEFAULTS.mexc_order_open_type,
    "mexc_recv_window": DEFAULTS.mexc_recv_window,
    "margin_allocation_enabled": DEFAULTS.margin_allocation_enabled,
    "mexc_api_key": "",
    "mexc_api_secret": "",
    "websocket_enabled": True,
    "production_gate_enabled": True,
    "weak_momentum_filter_enabled": os.getenv("WEAK_MOMENTUM_FILTER_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
    "momentum_min_5m_confirm_pct": float(os.getenv("MOMENTUM_MIN_5M_CONFIRM_PCT", "0.05")),
    "momentum_min_imbalance_abs": float(os.getenv("MOMENTUM_MIN_IMBALANCE_ABS", "0.02")),
    "momentum_max_spread_pct": float(os.getenv("MOMENTUM_MAX_SPREAD_PCT", "0.12")),

    "openai_analysis_enabled": os.getenv("OPENAI_ANALYSIS_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
    "openai_model": os.getenv("OPENAI_MODEL", "gpt-5.4-mini"),
    "openai_check_strength": os.getenv("OPENAI_CHECK_STRENGTH", "medium"),
    "openai_api_key": "",
    "openai_env_fallback": os.getenv("OPENAI_ENV_FALLBACK", "true").lower() in {"1", "true", "yes", "on"},
    "openai_timeout_sec": int(os.getenv("OPENAI_TIMEOUT_SEC", "12")),
    "openai_fail_open": os.getenv("OPENAI_FAIL_OPEN", "false").lower() in {"1", "true", "yes", "on"},
    "openai_show_decisions": os.getenv("OPENAI_SHOW_DECISIONS", "false").lower() in {"1", "true", "yes", "on"},
    "trade_charts_enabled": os.getenv("TRADE_CHARTS_ENABLED", "false").lower() in {"1", "true", "yes", "on"},

    "ws_enabled": True,
    "ws_require_healthy_for_entries": False,
    "ws_stale_sec": 20,
    "settings_revision": 1,
    "total_positions_opened": int(os.getenv("TOTAL_POSITIONS_OPENED", "0") or 0),
    "ai_scalping_session_id": int(os.getenv("AI_SCALPING_SESSION_ID", "1") or 1),
    "ai_scalping_session_reset_at": float(os.getenv("AI_SCALPING_SESSION_RESET_AT", "0") or 0),
    "ai_scalping_prev_scan_interval_sec": int(os.getenv("AI_SCALPING_PREV_SCAN_INTERVAL_SEC", "0") or 0),
}

class Storage:
    def __init__(self, path: str = DB_PATH):
        self.path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_open REAL,
                ts_close REAL,
                symbol TEXT,
                side TEXT,
                strategy TEXT,
                mode TEXT,
                entry_price REAL,
                exit_price REAL,
                qty REAL,
                pnl_usdt REAL,
                pnl_pct REAL,
                result TEXT,
                reason TEXT,
                mirror_used INTEGER DEFAULT 0,
                session TEXT,
                raw TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                side TEXT,
                status TEXT,
                entry_price REAL,
                qty REAL,
                stop_price REAL,
                take_price REAL,
                strategy TEXT,
                order_id TEXT,
                tp_order_id TEXT,
                sl_order_id TEXT,
                opened_at REAL,
                updated_at REAL,
                raw TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS locks (
                symbol TEXT PRIMARY KEY,
                locked_until REAL,
                reason TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS ai_scalping_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                session_id INTEGER NOT NULL,
                symbol TEXT,
                event TEXT NOT NULL,
                reason TEXT,
                confidence REAL,
                model TEXT,
                raw TEXT
            )
            """)
            await db.commit()
        for k, v in DEFAULT_SETTINGS.items():
            if await self.get(k) is None:
                await self.set(k, v, bump_revision=False)

        # v75: protect API keys from disappearing when SQLite/default settings are missing
        # or Railway restarts with only env/backup available. This does not change
        # trading logic; it only mirrors existing env/backup secrets into runtime settings.
        try:
            apply_secret_backup_to_env()
            env_mexc_key = str(os.getenv("MEXC_API_KEY", "") or "").strip()
            env_mexc_secret = str(os.getenv("MEXC_API_SECRET", "") or "").strip()
            env_openai_key = str(os.getenv("OPENAI_API_KEY", "") or "").strip()
            if env_mexc_key and not str(await self.get("mexc_api_key", "") or "").strip():
                await self.set("mexc_api_key", env_mexc_key, bump_revision=False)
            if env_mexc_secret and not str(await self.get("mexc_api_secret", "") or "").strip():
                await self.set("mexc_api_secret", env_mexc_secret, bump_revision=False)
            if env_openai_key and not str(await self.get("openai_api_key", "") or "").strip():
                await self.set("openai_api_key", env_openai_key, bump_revision=False)
            if env_mexc_key or env_mexc_secret or env_openai_key:
                save_secret_backup({
                    "mexc_api_key": env_mexc_key,
                    "mexc_api_secret": env_mexc_secret,
                    "openai_api_key": env_openai_key,
                })
        except Exception:
            pass

        # One-time safety migration for older deployments where these values
        # may already exist in the DB and therefore are not replaced by defaults.
        # MEXC push.tickers sends the full futures universe, so batch=250 makes
        # the cache incomplete and looks like a broken websocket.
        try:
            if int(await self.get("ws_max_updates_per_batch", 1000) or 1000) < 1000:
                await self.set("ws_max_updates_per_batch", 1000, bump_revision=False)
            if int(await self.get("ws_stale_sec", 20) or 20) < 20:
                await self.set("ws_stale_sec", 20, bump_revision=False)
        except Exception:
            pass

        # v0222 migration: force the real fast-profit extraction values into
        # existing SQLite settings, otherwise old DB values keep overriding code
        # defaults and the bot still waits too long in profitable HUNTER positions.
        try:
            if await self.get("v0222_fast_profit_real_migrated") is None:
                await self.set("boost_fast_profit_enabled", True, bump_revision=False)
                await self.set("boost_fast_profit_min_pct", 0.11, bump_revision=False)
                await self.set("boost_fast_profit_exchange_min_pct", 0.09, bump_revision=False)
                await self.set("boost_fast_profit_min_age_sec", 2, bump_revision=False)
                await self.set("boost_fast_profit_max_hold_sec", 12, bump_revision=False)
                await self.set("boost_fast_trailing_start_pct", 0.022, bump_revision=False)
                await self.set("boost_fast_trailing_giveback_pct", 0.007, bump_revision=False)
                await self.set("boost_momentum_decay_profit_pct", 0.006, bump_revision=False)
                await self.set("boost_fast_profit_wait_event_cooldown_sec", 999999, bump_revision=False)
                await self.set("v0222_fast_profit_real_migrated", True, bump_revision=False)
        except Exception:
            pass
        # v0225: fee-aware BOOST profit gate. Tiny +0.01%/+0.03% moves can be
        # negative on the real balance after taker fees/slippage. Force live
        # profit extraction to wait for a fee-covered exchange move.
        try:
            if await self.get("v0225_fee_aware_profit_migrated") is None:
                await self.set("boost_fast_profit_min_pct", 0.11, bump_revision=False)
                await self.set("boost_fast_profit_exchange_min_pct", 0.09, bump_revision=False)
                await self.set("boost_live_min_exchange_profit_pct", 0.09, bump_revision=False)
                await self.set("boost_min_profit_to_rotate_pct", 0.09, bump_revision=False)
                await self.set("v0225_fee_aware_profit_migrated", True, bump_revision=False)
        except Exception:
            pass


        # v0279: existing Railway DB/settings could keep old cascade_hunter
        # thresholds from v0277/v0278 toggle defaults and override the stricter
        # code defaults. Force the requested strict cascade prefilter once.
        try:
            if await self.get("v0279_cascade_strict_settings_migrated") is None:
                await self.set("cascade_hunter_min_pressure_ratio", 0.070, bump_revision=False)
                await self.set("cascade_hunter_min_volume_ratio", 2.2, bump_revision=False)
                await self.set("cascade_hunter_min_price_move_pct", 0.45, bump_revision=False)
                await self.set("cascade_hunter_max_spread_pct", 0.12, bump_revision=False)
                await self.set("v0279_cascade_strict_settings_migrated", True, bump_revision=False)
        except Exception:
            pass

        # v0060 default migration: previous builds stored leverage=1 in SQLite,
        # so changing only DEFAULTS would not affect an existing Railway DB.
        # If the user did not set a Railway env override, move the old default
        # to the new requested default 5x once. Telegram /leverage can still
        # change it any time after startup.
        try:
            if await self.get("v0060_leverage_default_migrated") is None:
                current_lev = int(await self.get("mexc_order_leverage", DEFAULTS.mexc_order_leverage) or DEFAULTS.mexc_order_leverage)
                if current_lev == 1 and os.getenv("MEXC_ORDER_LEVERAGE") is None:
                    await self.set("mexc_order_leverage", 5, bump_revision=False)
                await self.set("v0060_leverage_default_migrated", True, bump_revision=False)
        except Exception:
            pass

        # v0280: add Strongest Coin simple mode defaults.
        try:
            if await self.get("v0280_strongest_coin_settings_migrated") is None:
                await self.set("strongest_coin_top_coins", 200, bump_revision=False)
                await self.set("strongest_coin_trade_margin_pct", 0.10, bump_revision=False)
                await self.set("strongest_coin_max_open_positions", 1, bump_revision=False)
                await self.set("strongest_coin_leverage", 10, bump_revision=False)
                await self.set("strongest_coin_cooldown_after_close_sec", 3600, bump_revision=False)
                await self.set("v0280_strongest_coin_settings_migrated", True, bump_revision=False)
        except Exception:
            pass

        # v0283: Strongest Coin stop/PnL fix. Existing Railway DBs may still
        # contain the old too-tight 0.60% stop values from v0280-v0282. Force the
        # safer defaults once so pressing the button is not required after deploy.
        try:
            if await self.get("v0283_strongest_coin_stop_pnl_migrated") is None:
                await self.set("strongest_coin_stop_buffer_pct", 0.25, bump_revision=False)
                await self.set("strongest_coin_min_sl_pct", 1.20, bump_revision=False)
                await self.set("strongest_coin_max_sl_pct", 2.20, bump_revision=False)
                await self.set("v0283_strongest_coin_stop_pnl_migrated", True, bump_revision=False)
        except Exception:
            pass

        # v0286: Strongest Coin real stop/hold fix. The earlier 1.2% minimum
        # stop was still too close for MEXC 10x momentum entries, and hold
        # detection could accept the same candle making the pullback low.
        # Force safer settings once for existing Railway DBs.
        try:
            if await self.get("v0286_strongest_coin_stop_hold_migrated") is None:
                await self.set("strongest_coin_stop_buffer_pct", 0.35, bump_revision=False)
                await self.set("strongest_coin_min_sl_pct", 1.60, bump_revision=False)
                await self.set("strongest_coin_max_sl_pct", 2.80, bump_revision=False)
                await self.set("strongest_coin_min_hold_recovery_pct", 0.30, bump_revision=False)
                await self.set("v0286_strongest_coin_stop_hold_migrated", True, bump_revision=False)
        except Exception:
            pass

    async def get(self, key: str, default: Any = None) -> Any:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
            row = await cur.fetchone()
            if not row:
                return default
            try:
                return json.loads(row[0])
            except Exception:
                return row[0]

    async def set(self, key: str, value: Any, bump_revision: bool = True) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, json.dumps(value), time.time()),
            )
            if bump_revision and key != "settings_revision":
                rev = int(await self.get("settings_revision", 1) or 1) + 1
                await db.execute(
                    "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                    ("settings_revision", json.dumps(rev), time.time()),
                )
            await db.commit()


    async def increment_counter(self, key: str, amount: int = 1) -> int:
        """Atomically increment a numeric setting and return the new value."""
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
            row = await cur.fetchone()
            try:
                current = int(json.loads(row[0])) if row and row[0] is not None else 0
            except Exception:
                current = 0
            new_value = current + int(amount or 0)
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, json.dumps(new_value), time.time()),
            )
            await db.commit()
            return new_value

    async def all_settings(self) -> dict:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT key,value FROM settings")
            rows = await cur.fetchall()
        out = {}
        for k, v in rows:
            try: out[k] = json.loads(v)
            except Exception: out[k] = v
        # v0046 safety migration: WS health is advisory only. Older Railway DBs
        # may contain ws_require_healthy_for_entries=true from previous builds;
        # forcing it false prevents stale websocket warnings from stopping scans
        # or blocking entries when REST/scanner data is usable.
        out["ws_require_healthy_for_entries"] = False
        # v75: secrets can come from SQLite, Railway env, or local backup file.
        # This prevents status/sync loops from seeing empty credentials after
        # transient DB/default reloads in Railway.
        try:
            out = merge_secrets_into_settings(out)
        except Exception:
            pass
        return out

    async def upsert_position(self, pos: dict) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            INSERT OR REPLACE INTO positions(symbol,side,status,entry_price,qty,stop_price,take_price,strategy,order_id,tp_order_id,sl_order_id,opened_at,updated_at,raw)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pos["symbol"], pos.get("side"), pos.get("status", "open"), pos.get("entry_price"),
                pos.get("qty"), pos.get("stop_price"), pos.get("take_price"), pos.get("strategy"),
                pos.get("order_id"), pos.get("tp_order_id"), pos.get("sl_order_id"),
                pos.get("opened_at", time.time()), time.time(), json.dumps(pos),
            ))
            await db.commit()

    async def remove_position(self, symbol: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            await db.commit()

    async def clear_positions(self) -> int:
        """Clear volatile local position cache.

        Exchange positions/orders are the source of truth for live management;
        this only removes SQLite cached rows, not trades/settings/API keys.
        """
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM positions")
            row = await cur.fetchone()
            count = int(row[0] or 0) if row else 0
            await db.execute("DELETE FROM positions")
            await db.commit()
            return count

    async def positions(self) -> list[dict]:
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT raw FROM positions")
            rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows if r and r[0]]

    async def position_symbols(self) -> set[str]:
        return {p["symbol"] for p in await self.positions()}

    async def add_trade(self, trade: dict) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            INSERT INTO trades(ts_open,ts_close,symbol,side,strategy,mode,entry_price,exit_price,qty,pnl_usdt,pnl_pct,result,reason,mirror_used,session,raw)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                trade.get("ts_open"), trade.get("ts_close", time.time()), trade.get("symbol"),
                trade.get("side"), trade.get("strategy"), trade.get("mode"),
                trade.get("entry_price"), trade.get("exit_price"), trade.get("qty"),
                trade.get("pnl_usdt"), trade.get("pnl_pct"), trade.get("result"),
                trade.get("reason"), 1 if trade.get("mirror_used") else 0, trade.get("session"),
                json.dumps(trade),
            ))
            await db.commit()

    async def trade_rows(self, since: float | None = None) -> list[dict]:
        q = "SELECT raw FROM trades"
        params = ()
        if since:
            q += " WHERE ts_close>=?"
            params = (since,)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(q, params)
            rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows if r and r[0]]


    async def add_ai_scalping_event(self, event: dict) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
            INSERT INTO ai_scalping_events(ts,session_id,symbol,event,reason,confidence,model,raw)
            VALUES(?,?,?,?,?,?,?,?)
            """, (
                float(event.get("ts", time.time())), int(event.get("session_id", 1) or 1),
                event.get("symbol"), event.get("event"), event.get("reason"),
                event.get("confidence"), event.get("model"), json.dumps(event),
            ))
            await db.commit()

    async def ai_scalping_events(self, since: float | None = None) -> list[dict]:
        q = "SELECT raw FROM ai_scalping_events"
        params = ()
        if since:
            q += " WHERE ts>=?"
            params = (since,)
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(q, params)
            rows = await cur.fetchall()
        out = []
        for r in rows:
            try:
                out.append(json.loads(r[0]))
            except Exception:
                pass
        return out

    async def set_lock(self, symbol: str, seconds: int, reason: str) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT OR REPLACE INTO locks(symbol,locked_until,reason) VALUES(?,?,?)", (symbol, time.time()+seconds, reason))
            await db.commit()

    async def is_locked(self, symbol: str) -> tuple[bool, str]:
        now = time.time()
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute("SELECT locked_until,reason FROM locks WHERE symbol=?", (symbol,))
            row = await cur.fetchone()
            if not row:
                return False, ""
            if row[0] <= now:
                await db.execute("DELETE FROM locks WHERE symbol=?", (symbol,))
                await db.commit()
                return False, ""
            return True, row[1] or "locked"
