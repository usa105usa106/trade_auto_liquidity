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
    if kind.lower().startswith("error") or "error" in kind.lower() or fields.get("ok") is False:
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
