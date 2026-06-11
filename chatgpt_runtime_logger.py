import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from claude_runtime_logger import mirror_to_claude_log

CHATGPT_RUNTIME_LOG_PATH = Path(os.getenv("CHATGPT_RUNTIME_LOG_PATH", "/tmp/chatgpt_mode_runtime.log"))
CHATGPT_RUNTIME_LOG_MAX_BYTES = int(os.getenv("CHATGPT_RUNTIME_LOG_MAX_BYTES", "900000") or 900000)


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def chatgpt_log_event(event: str, **fields: Any) -> None:
    """Append one JSON line to /tmp/chatgpt_mode_runtime.log.

    This logger is intentionally tiny and dependency-free so low-level modules
    can write to /log_chatgpt without importing chatgpt_mode.py and creating
    circular imports.
    """
    try:
        CHATGPT_RUNTIME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if CHATGPT_RUNTIME_LOG_PATH.exists() and CHATGPT_RUNTIME_LOG_PATH.stat().st_size > CHATGPT_RUNTIME_LOG_MAX_BYTES:
            rotated = CHATGPT_RUNTIME_LOG_PATH.with_suffix(CHATGPT_RUNTIME_LOG_PATH.suffix + ".1")
            try:
                if rotated.exists():
                    rotated.unlink()
                CHATGPT_RUNTIME_LOG_PATH.rename(rotated)
            except Exception:
                CHATGPT_RUNTIME_LOG_PATH.unlink(missing_ok=True)
        safe = {}
        for k, v in fields.items():
            try:
                txt = str(v)
            except Exception:
                txt = repr(v)
            safe[k] = txt[:2500]
        line = json.dumps({"ts": _now_utc(), "event": event, **safe}, ensure_ascii=False)
        with CHATGPT_RUNTIME_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        mirror_to_claude_log(event, **fields)
    except Exception:
        pass
