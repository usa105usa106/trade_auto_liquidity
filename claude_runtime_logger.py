import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CLAUDE_RUNTIME_LOG_PATH = Path(os.getenv("CLAUDE_RUNTIME_LOG_PATH", "/tmp/claude_autopilot_runtime.log"))
CLAUDE_RUNTIME_LOG_MAX_BYTES = int(os.getenv("CLAUDE_RUNTIME_LOG_MAX_BYTES", "5000000") or 5000000)
CLAUDE_RUNTIME_LOG_FIELD_MAX_CHARS = int(os.getenv("CLAUDE_RUNTIME_LOG_FIELD_MAX_CHARS", "50000") or 50000)


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def claude_runtime_log_path() -> str:
    CLAUDE_RUNTIME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(CLAUDE_RUNTIME_LOG_PATH)


def claude_log_event(event: str, **fields: Any) -> None:
    """Append one detailed JSON line to the Claude Autopilot runtime log.

    /log_claude returns this file.  It intentionally captures the whole Claude
    LIVE path: API key/source, scan pack, Claude request/response, setup
    parsing/validation, entry order placement, SL/TP placement and monitor
    reconciliation.  Secrets are never written: callers should pass only source
    flags or masked values.
    """
    try:
        path = CLAUDE_RUNTIME_LOG_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size > CLAUDE_RUNTIME_LOG_MAX_BYTES:
            rotated = path.with_suffix(path.suffix + ".1")
            try:
                if rotated.exists():
                    rotated.unlink()
                path.rename(rotated)
            except Exception:
                path.unlink(missing_ok=True)
        safe: dict[str, str] = {}
        for k, v in fields.items():
            # Never let common secret field names leak into /log_claude.
            lk = str(k).lower()
            if "api_key" in lk or "secret" in lk or "token" in lk or "password" in lk:
                safe[k] = "***"
                continue
            try:
                txt = str(v)
            except Exception:
                txt = repr(v)
            safe[k] = txt[:CLAUDE_RUNTIME_LOG_FIELD_MAX_CHARS]
        line = json.dumps({"ts": _now_utc(), "event": event, **safe}, ensure_ascii=False)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def mirror_to_claude_log(event: str, **fields: Any) -> None:
    """Mirror low-level ChatGPT executor/position events only during Claude LIVE.

    Claude Autopilot reuses the ChatGPT execution engine.  While a Claude cycle
    is active this mirrors those existing executor events into /log_claude, so
    the Claude log contains entries, SL/TP placement, pending limits and monitor
    errors without adding separate caches or changing trading behavior.
    """
    try:
        active = os.getenv("CLAUDE_AUTOPILOT_LOG_ACTIVE", "").strip()
        if active or str(event).startswith("claude_"):
            claude_log_event(event, claude_run_id=active, **fields)
    except Exception:
        pass


def tail_claude_runtime_log(max_lines: int = 120) -> str:
    try:
        path = Path(claude_runtime_log_path())
        if not path.exists():
            return "claude runtime log is empty"
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-max_lines:]) or "claude runtime log is empty"
    except Exception as e:
        return f"failed to read claude runtime log: {e}"
