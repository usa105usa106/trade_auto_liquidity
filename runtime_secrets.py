from __future__ import annotations

import os
from typing import Any, Dict

SENSITIVE_SETTING_KEYS = {
    "mexc_api_key": "MEXC_API_KEY",
    "mexc_api_secret": "MEXC_API_SECRET",
    "openai_api_key": "OPENAI_API_KEY",
}

# V79: one simple source inside a running bot process.
# No backup-file search, no multi-cache guessing.
# After redeploy/fresh install the user should set keys again with /api set and /openai set,
# or provide Railway Variables.
_RUNTIME_SECRET_CACHE: Dict[str, str] = {}

def set_runtime_secret_cache(values: Dict[str, Any]) -> None:
    for k, v in (values or {}).items():
        if k not in SENSITIVE_SETTING_KEYS:
            continue
        sv = str(v or "").strip()
        if sv:
            _RUNTIME_SECRET_CACHE[k] = sv
            os.environ[SENSITIVE_SETTING_KEYS[k]] = sv
        else:
            _RUNTIME_SECRET_CACHE.pop(k, None)
            os.environ.pop(SENSITIVE_SETTING_KEYS[k], None)

def clear_runtime_secret_cache(keys) -> None:
    for k in keys:
        kk = str(k)
        _RUNTIME_SECRET_CACHE.pop(kk, None)
        env = SENSITIVE_SETTING_KEYS.get(kk)
        if env:
            os.environ.pop(env, None)

def runtime_secret_cache() -> Dict[str, str]:
    return dict(_RUNTIME_SECRET_CACHE)

# Compatibility no-ops.  V79 intentionally does not search/write backup secret files.
def load_secret_backup() -> Dict[str, str]:
    return {}

def save_secret_backup(values: Dict[str, Any]) -> None:
    # Keep only the in-process/env copy for this deployment session.
    set_runtime_secret_cache(values or {})

def clear_secret_backup(keys) -> None:
    clear_runtime_secret_cache(keys)

def apply_secret_backup_to_env() -> None:
    return None

def _session_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    val = str((settings or {}).get(setting_key) or "").strip()
    if val:
        return val
    val = str(_RUNTIME_SECRET_CACHE.get(setting_key) or "").strip()
    if val:
        return val
    return str(os.getenv(env_key, "") or "").strip()

def secret_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    return _session_value(settings, setting_key, env_key)

def merge_secrets_into_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(settings or {})
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = _session_value(out, setting_key, env_key)
        if val:
            out[setting_key] = val
            _RUNTIME_SECRET_CACHE[setting_key] = val
            os.environ[env_key] = val
    return out

def ensure_runtime_secrets_loaded(settings: Dict[str, Any] | None = None) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = _session_value(settings, setting_key, env_key)
        if val:
            values[setting_key] = val
            _RUNTIME_SECRET_CACHE[setting_key] = val
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
    return {
        "sqlite_mexc_key": bool(str((settings or {}).get("mexc_api_key") or "").strip()),
        "sqlite_mexc_secret": bool(str((settings or {}).get("mexc_api_secret") or "").strip()),
        "sqlite_openai": bool(str((settings or {}).get("openai_api_key") or "").strip()),
        "runtime_mexc_key": bool(str(_RUNTIME_SECRET_CACHE.get("mexc_api_key") or "").strip()),
        "runtime_mexc_secret": bool(str(_RUNTIME_SECRET_CACHE.get("mexc_api_secret") or "").strip()),
        "runtime_openai": bool(str(_RUNTIME_SECRET_CACHE.get("openai_api_key") or "").strip()),
        "env_mexc_key": bool(str(os.getenv("MEXC_API_KEY", "") or "").strip()),
        "env_mexc_secret": bool(str(os.getenv("MEXC_API_SECRET", "") or "").strip()),
        "env_openai": bool(str(os.getenv("OPENAI_API_KEY", "") or "").strip()),
        "backup_paths": "disabled_v79",
    }
