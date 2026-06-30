import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG_DIR = Path(os.getenv("BOT_LOG_DIR", "logs"))
SENSITIVE_KEYS = {"apikey", "api_key", "secret", "signature", "authorization", "token"}


def _mask(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if str(k).lower() in SENSITIVE_KEYS:
                out[k] = "***"
            else:
                out[k] = _mask(v)
        return out
    if isinstance(obj, list):
        return [_mask(x) for x in obj]
    return obj


def _write(path: Path, record: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        rec = dict(record)
        rec.setdefault("ts", datetime.now(timezone.utc).isoformat())
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(_mask(rec), ensure_ascii=False, default=str) + "\n")
    except Exception:
        # Logging must never break trading.
        pass


def log_event(kind: str, **fields: Any) -> None:
    record = {"kind": kind, **fields}
    _write(LOG_DIR / "trade.log", record)
    low_kind = str(kind).lower()
    strategy = str(fields.get("strategy") or "").lower()
    if low_kind.startswith("btc_ai"):
        _write(LOG_DIR / "btc_ai.log", record)
    # v433_full Ratio Pressure: keep a dedicated full forensic log for /log_full.
    # It captures scan decisions, skips, order placement, SL/TP confirmation,
    # position sync/live-card updates and time-stop/TP/SL close events without
    # changing behaviour of any other mode.
    if low_kind.startswith("ratio_pressure") or strategy == "ratio_pressure_1h" or low_kind == "ratio_position_closed":
        _write(LOG_DIR / "ratio_pressure.log", record)
    if low_kind.startswith("error") or "error" in low_kind or fields.get("ok") is False:
        _write(LOG_DIR / "errors.log", record)


def log_mexc(method: str, path: str, request: dict | None = None, response: Any = None, status: int | None = None, error: Any = None) -> None:
    record = {
        "kind": "mexc_raw",
        "method": method,
        "path": path,
        "status": status,
        "request": request or {},
        "response": response,
    }
    if error is not None:
        record["error"] = str(error)
    _write(LOG_DIR / "mexc_raw.log", record)
    if error is not None or (isinstance(response, dict) and response.get("success") is False):
        _write(LOG_DIR / "errors.log", record)


def tail_text(files: list[str] | None = None, lines: int = 80, max_chars: int = 3500) -> str:
    files = files or ["errors.log", "mexc_raw.log", "trade.log"]
    chunks: list[str] = []
    for name in files:
        path = LOG_DIR / name
        if not path.exists():
            chunks.append(f"== {name} ==\n(empty)")
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as f:
                data = f.readlines()[-max(1, lines):]
            chunks.append(f"== {name} ==\n" + "".join(data))
        except Exception as e:
            chunks.append(f"== {name} ==\nread error: {e}")
    text = "\n".join(chunks)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text or "Логи пустые."


def tail_important(lines: int = 160, max_chars: int = 12000) -> str:
    """Return only actionable trading/protection logs for Telegram /log.

    Balance snapshots are huge and hide the important TP/SL payloads. This
    filter keeps MEXC order/protection endpoints plus errors/trade events.
    """
    important_paths = (
        "/api/v1/private/order/create",
        "/api/v1/private/planorder/place",
        "/api/v1/private/stoporder/place",
                "/api/v1/private/stoporder",
    )
    important_kinds = (
        "error", "protection", "tpsl", "trigger", "opened", "closed",
        "boost", "quick_bounce", "btc_ai", "prompt", "order", "scan", "scanner", "decision", "rejected", "wait", "stage",
        "entry", "open", "opened", "tp", "sl", "take", "stop", "virtual", "real_tpsl",
        "mexc_native", "mexc_trigger", "mexc_stoporder_place_body",
    )
    records: list[str] = []
    for name in ("errors.log", "trade.log", "mexc_raw.log"):
        path = LOG_DIR / name
        if not path.exists():
            continue
        try:
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-max(lines * 4, 200):]:
                keep = False
                try:
                    rec = json.loads(line)
                    kind = str(rec.get("kind", "")).lower()
                    rec_path = str(rec.get("path", ""))
                    if any(k in kind for k in important_kinds) or any(rec_path.startswith(p) for p in important_paths):
                        keep = True
                    # Drop massive non-actionable reads unless they are near protection.
                    if rec_path.endswith("/account/assets") or "account/assets" in rec_path:
                        keep = False
                except Exception:
                    low = line.lower()
                    keep = any(k in low for k in important_kinds)
                if keep:
                    records.append(f"== {name} == {line}")
        except Exception as e:
            records.append(f"== {name} == read error: {e}")
    if not records:
        return "Нет важных ошибок TP/SL/ордеров в логах. Попробуй /log после следующей сделки."
    text = "\n".join(records[-max(1, lines):])
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text
