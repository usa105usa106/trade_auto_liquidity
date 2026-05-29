from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict

SENSITIVE_SETTING_KEYS = {
    "mexc_api_key": "MEXC_API_KEY",
    "mexc_api_secret": "MEXC_API_SECRET",
    "openai_api_key": "OPENAI_API_KEY",
}

# v76: in-process fallback. Railway/Telegram can route consecutive updates while
# SQLite/backup/env are being refreshed; keep the last /api set and /openai set
# values in memory too, so immediate next commands such as /status_btc cannot
# see empty credentials inside the same running process.
_RUNTIME_SECRET_CACHE: Dict[str, str] = {}

def set_runtime_secret_cache(values: Dict[str, Any]) -> None:
    for k, v in (values or {}).items():
        if k not in SENSITIVE_SETTING_KEYS:
            continue
        sv = str(v or '').strip()
        if sv:
            _RUNTIME_SECRET_CACHE[k] = sv
        else:
            _RUNTIME_SECRET_CACHE.pop(k, None)

def clear_runtime_secret_cache(keys: list[str] | tuple[str, ...] | set[str]) -> None:
    for k in keys:
        _RUNTIME_SECRET_CACHE.pop(str(k), None)

def runtime_secret_cache() -> Dict[str, str]:
    return dict(_RUNTIME_SECRET_CACHE)


def _secret_paths() -> list[Path]:
    """Return all fallback files for secrets, ordered by priority.

    V77: one process can read settings from SQLite while another command uses
    env/cache/backup.  Keep a small redundant backup list so MEXC/OpenAI keys
    do not randomly disappear after background tests, restarts, or DB path changes.
    Do not rely on this instead of Railway Variables for a fresh redeploy, but it
    makes the running bot stable.
    """
    paths: list[Path] = []
    for raw in (os.getenv("BOT_SECRETS_PATH"), os.getenv("SECRETS_PATH")):
        if raw:
            paths.append(Path(raw))
    try:
        from config import DB_PATH
        base = Path(DB_PATH)
        if base.name:
            paths.append(base.with_suffix(base.suffix + ".secrets.json"))
    except Exception:
        pass
    paths.extend([
        Path("bot_secrets.json"),
        Path("/tmp/bot_secrets.json"),
    ])
    seen: set[str] = set()
    out: list[Path] = []
    for p in paths:
        try:
            key = str(p.expanduser().resolve())
        except Exception:
            key = str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _secret_path() -> Path:
    return _secret_paths()[0]


def load_secret_backup() -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for path in _secret_paths():
        try:
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                continue
            for k, v in data.items():
                if k in SENSITIVE_SETTING_KEYS and str(v or "").strip() and not merged.get(str(k)):
                    merged[str(k)] = str(v).strip()
        except Exception:
            continue
    if merged:
        set_runtime_secret_cache(merged)
    return merged


def save_secret_backup(values: Dict[str, Any]) -> None:
    set_runtime_secret_cache(values)
    path = _secret_path()
    current = load_secret_backup()
    for k, v in (values or {}).items():
        if k not in SENSITIVE_SETTING_KEYS:
            continue
        sv = str(v or "").strip()
        if sv:
            current[k] = sv
        else:
            current.pop(k, None)
    for path in _secret_paths():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
            try:
                os.chmod(tmp, 0o600)
            except Exception:
                pass
            tmp.replace(path)
        except Exception:
            # Do not break trading/storage commands because a fallback file could not be written.
            continue


def clear_secret_backup(keys: list[str] | tuple[str, ...] | set[str]) -> None:
    clear_runtime_secret_cache(keys)
    current = load_secret_backup()
    for k in keys:
        current.pop(str(k), None)
    for path in _secret_paths():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
            try:
                os.chmod(path, 0o600)
            except Exception:
                pass
        except Exception:
            continue


def apply_secret_backup_to_env() -> None:
    data = load_secret_backup()
    if data:
        set_runtime_secret_cache(data)
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = str(data.get(setting_key) or "").strip()
        if val and not os.getenv(env_key):
            os.environ[env_key] = val


def secret_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    # Priority: Telegram/SQLite settings -> runtime in-process cache -> Railway env -> backup file.
    val = str((settings or {}).get(setting_key) or "").strip()
    if val:
        return val
    val = str(_RUNTIME_SECRET_CACHE.get(setting_key) or "").strip()
    if val:
        return val
    val = str(os.getenv(env_key, "") or "").strip()
    if val:
        return val
    val = str(load_secret_backup().get(setting_key) or "").strip()
    if val:
        _RUNTIME_SECRET_CACHE[setting_key] = val
    return val


def merge_secrets_into_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(settings or {})
    backup = load_secret_backup()
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        if not str(out.get(setting_key) or "").strip():
            cache_val = str(_RUNTIME_SECRET_CACHE.get(setting_key) or "").strip()
            env_val = str(os.getenv(env_key, "") or "").strip()
            if cache_val:
                out[setting_key] = cache_val
            elif env_val:
                out[setting_key] = env_val
            elif backup.get(setting_key):
                out[setting_key] = backup[setting_key]
                _RUNTIME_SECRET_CACHE[setting_key] = str(backup[setting_key])
    return out


def ensure_runtime_secrets_loaded(settings: Dict[str, Any] | None = None) -> Dict[str, str]:
    """Refresh in-process/env secret cache from every source.

    This is intentionally cheap and safe to call before every exchange/OpenAI
    operation.  It prevents background tasks from seeing empty credentials after
    settings reloads or when SQLite has not yet been updated.
    """
    values: Dict[str, str] = {}
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = str((settings or {}).get(setting_key) or "").strip()
        if not val:
            val = str(_RUNTIME_SECRET_CACHE.get(setting_key) or "").strip()
        if not val:
            val = str(os.getenv(env_key, "") or "").strip()
        if val:
            values[setting_key] = val
    backup = load_secret_backup()
    for setting_key in SENSITIVE_SETTING_KEYS:
        if not values.get(setting_key) and backup.get(setting_key):
            values[setting_key] = str(backup[setting_key]).strip()
    if values:
        set_runtime_secret_cache(values)
        for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
            val = values.get(setting_key)
            if val:
                os.environ[env_key] = val
    return values


def mask_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "missing"
    if len(value) <= 8:
        return "saved"
    return f"{value[:4]}...{value[-4:]}"
