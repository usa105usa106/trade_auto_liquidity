from __future__ import annotations

import os
from typing import Any, Dict

SENSITIVE_SETTING_KEYS = {
    "mexc_api_key": "MEXC_API_KEY",
    "mexc_api_secret": "MEXC_API_SECRET",
    "openai_api_key": "OPENAI_API_KEY",
}

# V11 simple secret rule:
# 1) SQLite settings are the main source after /api set and /openai set.
# 2) Railway/process ENV is only a fallback when SQLite is empty.
# 3) No backup files, no old caches, no restart cache restore.
# 4) When Telegram commands save keys, they save to SQLite and mirror to os.environ
#    only for the current running process.


def _clean(v: Any) -> str:
    return str(v or "").strip()


def set_runtime_secret_cache(values: Dict[str, Any]) -> None:
    """Compatibility function: mirror fresh Telegram-saved keys to process env only."""
    for k, v in (values or {}).items():
        env_key = SENSITIVE_SETTING_KEYS.get(k)
        if not env_key:
            continue
        sv = _clean(v)
        if sv:
            os.environ[env_key] = sv
        else:
            os.environ.pop(env_key, None)


def clear_runtime_secret_cache(keys) -> None:
    for k in keys:
        env = SENSITIVE_SETTING_KEYS.get(str(k))
        if env:
            os.environ.pop(env, None)


def runtime_secret_cache() -> Dict[str, str]:
    return {}


# Compatibility no-ops. V8 intentionally does NOT read/write persistent backup/cache files.
def load_secret_backup() -> Dict[str, str]:
    return {}


def save_secret_backup(values: Dict[str, Any]) -> None:
    # V11: compatibility no-op. Telegram commands save directly to SQLite.
    set_runtime_secret_cache(values or {})


def clear_secret_backup(keys) -> None:
    clear_runtime_secret_cache(keys)


def apply_secret_backup_to_env() -> None:
    # No old-cache/backup restore after restart.
    return None


def _session_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    # IMPORTANT V8: SQLite first. ENV only fallback if SQLite is empty.
    val = _clean((settings or {}).get(setting_key))
    if val:
        return val
    val = _clean(os.getenv(env_key, ""))
    if val:
        return val
    return ""


def secret_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    return _session_value(settings, setting_key, env_key)


def merge_secrets_into_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Return settings with simple deterministic secrets applied.

    SQLite values are never overwritten by ENV here. If SQLite is empty and ENV is
    present, ENV is exposed in the returned dict as fallback for this process.
    """
    out = dict(settings or {})
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = _session_value(out, setting_key, env_key)
        if val:
            out[setting_key] = val
            os.environ[env_key] = val
    return out


def ensure_runtime_secrets_loaded(settings: Dict[str, Any] | None = None) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = _session_value(settings, setting_key, env_key)
        if val:
            values[setting_key] = val
            os.environ[env_key] = val
    return values


def mask_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "missing"
    if len(value) <= 8:
        return "saved"
    return f"{value[:4]}...{value[-4:]}"


def secret_source_report(settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    s = settings or {}
    sqlite_mk = bool(_clean(s.get("mexc_api_key")))
    sqlite_ms = bool(_clean(s.get("mexc_api_secret")))
    sqlite_oa = bool(_clean(s.get("openai_api_key")))
    env_mk = bool(_clean(os.getenv("MEXC_API_KEY", "")))
    env_ms = bool(_clean(os.getenv("MEXC_API_SECRET", "")))
    env_oa = bool(_clean(os.getenv("OPENAI_API_KEY", "")))
    return {
        "source_priority": "sqlite primary; env/runtime only repair fallback; no backup/cache files",
        "sqlite_mexc_key": sqlite_mk,
        "sqlite_mexc_secret": sqlite_ms,
        "sqlite_openai": sqlite_oa,
        "env_mexc_key": env_mk,
        "env_mexc_secret": env_ms,
        "env_openai": env_oa,
        "active_mexc_key_source": "sqlite" if sqlite_mk else "env" if env_mk else "missing",
        "active_mexc_secret_source": "sqlite" if sqlite_ms else "env" if env_ms else "missing",
        "active_openai_source": "sqlite" if sqlite_oa else "env" if env_oa else "missing",
        "backup_paths": "disabled_v11_no_cache",
    }
