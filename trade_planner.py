import os
from models import TradePlan


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


class TradePlanner:
    def __init__(self):
        self.default_equity = float(os.getenv("DEFAULT_EQUITY_USDT", "1000"))
        self.min_order_usdt = float(os.getenv("MIN_ORDER_USDT", "5"))
        self.max_order_usdt = float(os.getenv("MAX_ORDER_USDT", "100"))
        self.tp_atr_mult = float(os.getenv("TP_ATR_MULT", "2.2"))
        self.sl_atr_mult = float(os.getenv("SL_ATR_MULT", "1.2"))
        # v0078: default risk model is a scalp profile, not swing.
        # Values are in raw price %, before leverage.
        self.min_tp_pct = float(os.getenv("MIN_TP_PCT", "0.12"))
        self.max_tp_pct = float(os.getenv("MAX_TP_PCT", "0.30"))
        self.min_sl_pct = float(os.getenv("MIN_SL_PCT", "0.20"))
        self.max_sl_pct = float(os.getenv("MAX_SL_PCT", "0.45"))
        # Fee-aware entry filter. Defaults are conservative % estimates for a
        # market-in/market-out scalp: taker fee each side + spread/slippage
        # buffer + minimal net profit. If the configured TP cannot clear this,
        # the plan is skipped instead of opening a mathematically bad scalp.
        self.taker_fee_pct = float(os.getenv("TAKER_FEE_PCT", "0.04"))
        self.spread_buffer_pct = float(os.getenv("SCALP_SPREAD_BUFFER_PCT", "0.03"))
        self.min_net_profit_pct = float(os.getenv("MIN_NET_PROFIT_PCT", "0.04"))
        # v0079: strategy risk profiles. Momentum remains the original scalp
        # profile. Pullback/reversal use slightly different bands instead of
        # silently reusing exactly the same TP/SL as momentum. All values are
        # raw price %, before leverage, and can be overridden from ENV.
        self.strategy_profiles = {
            "momentum": {
                "min_tp": float(os.getenv("MOMENTUM_MIN_TP_PCT", os.getenv("MIN_TP_PCT", "0.12"))),
                "max_tp": float(os.getenv("MOMENTUM_MAX_TP_PCT", os.getenv("MAX_TP_PCT", "0.30"))),
                "min_sl": float(os.getenv("MOMENTUM_MIN_SL_PCT", os.getenv("MIN_SL_PCT", "0.20"))),
                "max_sl": float(os.getenv("MOMENTUM_MAX_SL_PCT", os.getenv("MAX_SL_PCT", "0.45"))),
                "tp_mult": float(os.getenv("MOMENTUM_TP_ATR_MULT", os.getenv("TP_ATR_MULT", "2.2"))),
                "sl_mult": float(os.getenv("MOMENTUM_SL_ATR_MULT", os.getenv("SL_ATR_MULT", "1.2"))),
            },
            "pullback": {
                "min_tp": float(os.getenv("PULLBACK_MIN_TP_PCT", "0.16")),
                "max_tp": float(os.getenv("PULLBACK_MAX_TP_PCT", "0.38")),
                "min_sl": float(os.getenv("PULLBACK_MIN_SL_PCT", "0.22")),
                "max_sl": float(os.getenv("PULLBACK_MAX_SL_PCT", "0.50")),
                "tp_mult": float(os.getenv("PULLBACK_TP_ATR_MULT", "2.0")),
                "sl_mult": float(os.getenv("PULLBACK_SL_ATR_MULT", "1.3")),
            },
            "reversal": {
                "min_tp": float(os.getenv("REVERSAL_MIN_TP_PCT", "0.18")),
                "max_tp": float(os.getenv("REVERSAL_MAX_TP_PCT", "0.45")),
                "min_sl": float(os.getenv("REVERSAL_MIN_SL_PCT", "0.25")),
                "max_sl": float(os.getenv("REVERSAL_MAX_SL_PCT", "0.60")),
                "tp_mult": float(os.getenv("REVERSAL_TP_ATR_MULT", "1.8")),
                "sl_mult": float(os.getenv("REVERSAL_SL_ATR_MULT", "1.4")),
            },

            "ai_scalping": {
                # Default fallback is BTC scalping; ETH uses per-symbol settings below.
                "min_tp": float(os.getenv("AI_SCALPING_TP_PCT", "0.18")),
                "max_tp": float(os.getenv("AI_SCALPING_TP_PCT", "0.18")),
                "min_sl": float(os.getenv("AI_SCALPING_SL_PCT", "0.26")),
                "max_sl": float(os.getenv("AI_SCALPING_SL_PCT", "0.26")),
                "tp_mult": 1.0,
                "sl_mult": 1.0,
            },
            "liquidity_retest": {
                # v0082: not a scalp profile. SL comes from the liquidity zone/wick,
                # TP is adaptive RR (2R/3R/4R). These bands are safety clamps only.
                "min_tp": float(os.getenv("LIQUIDITY_RETEST_MIN_TP_PCT", "0.35")),
                "max_tp": float(os.getenv("LIQUIDITY_RETEST_MAX_TP_PCT", "5.00")),
                "min_sl": float(os.getenv("LIQUIDITY_RETEST_MIN_SL_PCT", "0.15")),
                "max_sl": float(os.getenv("LIQUIDITY_RETEST_MAX_SL_PCT", "1.20")),
                "tp_mult": float(os.getenv("LIQUIDITY_RETEST_DEFAULT_RR", "3.0")),
                "sl_mult": float(os.getenv("LIQUIDITY_RETEST_SL_ATR_MULT", "1.0")),
            },
        }

    def _profile_for(self, strategy: str) -> dict:
        return self.strategy_profiles.get(str(strategy or "momentum").lower(), self.strategy_profiles["momentum"])

    @staticmethod
    def _bool_setting(settings: dict, key: str, default: bool = True) -> bool:
        raw = settings.get(key, os.getenv(key.upper(), default))
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def make_plan(self, candidate: dict, settings: dict, equity_usdt: float | None = None) -> TradePlan | None:
        price = float(candidate.get("futures_price") or 0)
        side = str(candidate.get("side", "")).upper()
        if price <= 0 or side not in {"LONG", "SHORT"}:
            return None

        equity = float(equity_usdt or self.default_equity)
        risk_pct = float(candidate.get("risk_pct", settings.get("risk_pct", 0.005)))
        atr_pct = float(candidate.get("atr_pct") or 0.25)
        strategy = str(candidate.get("strategy", "momentum")).lower()
        profile = self._profile_for(strategy)
        details = candidate.get("score_details") or {}
        if strategy == "liquidity_retest":
            # v0082: SL is placed behind the detected demand/supply zone. TP is
            # adaptive RR: 2R for weak/okay retests, 3R normal, 4R strong setups.
            zone_low = float(details.get("zone_low") or 0)
            zone_high = float(details.get("zone_high") or 0)
            buffer_pct = float(settings.get("liquidity_retest_sl_buffer_pct", os.getenv("LIQUIDITY_RETEST_SL_BUFFER_PCT", "0.04")) or 0.04)
            rr = float(details.get("adaptive_rr") or settings.get("liquidity_retest_default_rr", os.getenv("LIQUIDITY_RETEST_DEFAULT_RR", "3.0")) or 3.0)
            rr = clamp(rr, 2.0, 4.0)
            if side == "LONG" and zone_low > 0:
                stop_from_zone = zone_low * (1 - buffer_pct / 100.0)
                sl_pct = max(0.0001, (price - stop_from_zone) / price * 100.0)
            elif side == "SHORT" and zone_high > 0:
                stop_from_zone = zone_high * (1 + buffer_pct / 100.0)
                sl_pct = max(0.0001, (stop_from_zone - price) / price * 100.0)
            else:
                sl_pct = clamp(atr_pct * float(profile["sl_mult"]), float(profile["min_sl"]), float(profile["max_sl"]))
            sl_pct = clamp(sl_pct, float(profile["min_sl"]), float(profile["max_sl"]))
            tp_pct = clamp(sl_pct * rr, float(profile["min_tp"]), float(profile["max_tp"]))
            # v0083: when the SMC detector found a real nearby liquidity target,
            # use it if it is inside the safe adaptive RR band. Otherwise keep
            # the adaptive 2R/3R/4R target so the trade stays structured.
            liquidity_target = float(details.get("liquidity_target") or 0) if isinstance(details, dict) else 0.0
            if liquidity_target > 0:
                if side == "LONG" and liquidity_target > price:
                    target_pct = (liquidity_target - price) / price * 100.0
                    target_rr = target_pct / sl_pct if sl_pct > 0 else 0.0
                    if 2.0 <= target_rr <= 4.25:
                        tp_pct = clamp(target_pct, float(profile["min_tp"]), float(profile["max_tp"]))
                        rr = clamp(target_rr, 2.0, 4.0)
                elif side == "SHORT" and liquidity_target < price:
                    target_pct = (price - liquidity_target) / price * 100.0
                    target_rr = target_pct / sl_pct if sl_pct > 0 else 0.0
                    if 2.0 <= target_rr <= 4.25:
                        tp_pct = clamp(target_pct, float(profile["min_tp"]), float(profile["max_tp"]))
                        rr = clamp(target_rr, 2.0, 4.0)
            candidate["liquidity_retest_rr"] = rr
        elif strategy == "ai_scalping":
            # v0126: AI no longer opens on direction alone. The engine attaches
            # structure/ATR based distances after a sweep/reclaim setup gate.
            # Keep old fixed env/settings only as fallback for legacy candidates.
            adaptive_tp = candidate.get("ai_scalping_tp_pct")
            adaptive_sl = candidate.get("ai_scalping_sl_pct")
            if adaptive_tp is not None and adaptive_sl is not None:
                tp_pct = max(0.01, float(adaptive_tp or 0.0))
                sl_pct = max(0.01, float(adaptive_sl or 0.0))
            else:
                sym_key = str(candidate.get("symbol") or "").upper().replace("/", "_").replace(":USDT", "")
                if sym_key.startswith("ETH_USDT"):
                    tp_default = os.getenv("AI_SCALPING_ETH_TP_PCT", "0.22")
                    sl_default = os.getenv("AI_SCALPING_ETH_SL_PCT", "0.32")
                    tp_setting = "ai_scalping_eth_tp_pct"
                    sl_setting = "ai_scalping_eth_sl_pct"
                else:
                    tp_default = os.getenv("AI_SCALPING_BTC_TP_PCT", os.getenv("AI_SCALPING_TP_PCT", "0.18"))
                    sl_default = os.getenv("AI_SCALPING_BTC_SL_PCT", os.getenv("AI_SCALPING_SL_PCT", "0.26"))
                    tp_setting = "ai_scalping_btc_tp_pct"
                    sl_setting = "ai_scalping_btc_sl_pct"
                tp_pct = max(0.01, float(settings.get(tp_setting, tp_default) or tp_default))
                sl_pct = max(0.01, float(settings.get(sl_setting, sl_default) or sl_default))
        else:
            sl_pct = clamp(atr_pct * float(profile["sl_mult"]), float(profile["min_sl"]), float(profile["max_sl"]))
            tp_pct = clamp(atr_pct * float(profile["tp_mult"]), float(profile["min_tp"]), float(profile["max_tp"]))
            min_fee_aware_tp = max(float(profile["min_tp"]), self.taker_fee_pct * 2 + self.spread_buffer_pct + self.min_net_profit_pct)
            if tp_pct < min_fee_aware_tp:
                tp_pct = min_fee_aware_tp
            if tp_pct > float(profile["max_tp"]):
                # TP would be too small after fees/spread for the configured strategy
                # band. Do not open; better no trade than a negative-expectancy setup.
                return None
        max_positions = max(1, int(candidate.get("max_open_positions", settings.get("max_open_positions", 5)) or 5))
        leverage = max(1, int(float(settings.get("mexc_order_leverage", os.getenv("MEXC_ORDER_LEVERAGE", "5")) or 5)))

        liq_stop_mode = (strategy == "ai_scalping") and self._bool_setting(settings, "ai_scalping_liquidation_stop_mode", False)
        risk_usdt = max(0.0, equity * risk_pct)
        stop_distance = price * (sl_pct / 100.0)
        if stop_distance <= 0:
            return None

        # Classic risk sizing: position notional based on loss at SL.
        risk_qty = risk_usdt / stop_distance
        risk_notional = risk_qty * price

        # v0064 safety: fixed margin allocation per slot.
        # Example: 50 USDT balance / 5 max positions = 10 USDT max margin.
        # With 5x leverage, max notional for one trade = 50 USDT.
        max_margin_per_position = equity / max_positions if max_positions > 0 else equity

        if liq_stop_mode:
            # v0116: AI BTC/ETH only high-risk mode. No exchange SL is placed.
            # The planned SL distance is converted into high leverage so the
            # isolated liquidation area acts as the hard loss boundary. This is
            # approximate because the exact liquidation price depends on MEXC
            # maintenance margin, mark price and fees, so we add a configurable
            # buffer behind the planned SL and cap leverage.
            margin_pct = max(0.001, min(1.0, float(settings.get("ai_scalping_liq_margin_pct", os.getenv("AI_SCALPING_LIQ_MARGIN_PCT", "0.05")) or 0.05)))
            buffer_pct = max(0.0, float(settings.get("ai_scalping_liq_buffer_pct", os.getenv("AI_SCALPING_LIQ_BUFFER_PCT", "0.04")) or 0.04))
            max_lev = max(1, int(float(settings.get("ai_scalping_liq_max_leverage", os.getenv("AI_SCALPING_LIQ_MAX_LEVERAGE", "200")) or 200)))
            target_liq_distance_pct = max(0.05, sl_pct + buffer_pct)
            leverage = max(1, min(max_lev, int(100.0 / target_liq_distance_pct)))
            margin_usdt = min(max_margin_per_position, max(self.min_order_usdt / max(1, leverage), equity * margin_pct))
            notional = min(self.max_order_usdt, max(self.min_order_usdt, margin_usdt * leverage))
            expected_margin = notional / leverage if leverage > 0 else notional
        else:
            max_notional_by_margin = max_margin_per_position * leverage
            notional_ceiling = self.max_order_usdt
            if self._bool_setting(settings, "margin_allocation_enabled", True):
                notional_ceiling = min(notional_ceiling, max_notional_by_margin)

            if notional_ceiling < self.min_order_usdt:
                # Account is too small for the configured number of slots/leverage/min order.
                return None

            notional = clamp(risk_notional, self.min_order_usdt, notional_ceiling)
            expected_margin = notional / leverage if leverage > 0 else notional
        qty = notional / price

        if side == "LONG":
            stop = price * (1 - sl_pct / 100.0)
            take = price * (1 + tp_pct / 100.0)
        else:
            stop = price * (1 + sl_pct / 100.0)
            take = price * (1 - tp_pct / 100.0)

        order_type = "market" if strategy in {"momentum", "ai_scalping"} else "limit"
        lr_rr = float(candidate.get("liquidity_retest_rr") or (details.get("adaptive_rr") if isinstance(details, dict) else 0) or 0)
        lr_zone_low = float(details.get("zone_low") or 0) if isinstance(details, dict) else 0.0
        lr_zone_high = float(details.get("zone_high") or 0) if isinstance(details, dict) else 0.0
        lr_reason = str(details.get("rr_reason") or details.get("setup") or "") if isinstance(details, dict) else ""
        return TradePlan(
            symbol=candidate["symbol"],
            side=side,
            order_type=order_type,
            qty=qty,
            entry_price=price,
            stop_price=stop,
            take_price=take,
            risk_pct=risk_pct,
            confidence=float(candidate.get("confidence", 0)),
            strategy=strategy,
            mirror_used=bool(candidate.get("mirror_used", False)),
            session=str(candidate.get("session", "NORMAL")),
            max_open_positions=max_positions,
            planned_notional_usdt=notional,
            expected_margin_usdt=expected_margin,
            max_margin_per_position_usdt=max_margin_per_position,
            leverage=leverage,
            liquidation_stop_mode=liq_stop_mode,
            liquidation_buffer_pct=(float(settings.get("ai_scalping_liq_buffer_pct", os.getenv("AI_SCALPING_LIQ_BUFFER_PCT", "0.04")) or 0.04) if liq_stop_mode else 0.0),
            liquidation_target_distance_pct=(sl_pct + float(settings.get("ai_scalping_liq_buffer_pct", os.getenv("AI_SCALPING_LIQ_BUFFER_PCT", "0.04")) or 0.04) if liq_stop_mode else 0.0),
            liquidity_retest_rr=lr_rr,
            liquidity_retest_zone_low=lr_zone_low,
            liquidity_retest_zone_high=lr_zone_high,
            liquidity_retest_reason=lr_reason,
        )
