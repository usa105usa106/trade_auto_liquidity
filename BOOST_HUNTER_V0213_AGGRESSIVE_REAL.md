# v0213 aggressive hunter real code

Applied to real settings and engine defaults, not only comments/markers.

Key runtime defaults forced on /boost_start:
- boost_futures_momentum_min_pct = 0.028
- boost_live_slippage_buffer_pct = 0.018
- boost_spread_edge_mult = 1.6
- boost_hunter_min_move_3m_pct = 0.095
- boost_hunter_min_accel_pct = 0.012
- boost_hunter_min_score = 82
- boost_hunter_entry_confirmations = 1
- boost_live_min_exchange_profit_pct = 0.055

Purpose: enter more often than defensive HUNTER while keeping no-trade/anti-chop, rate-limit safe scan, exchange sync, UNSAFE defensive mode, and STOP BOOST shutdown.
