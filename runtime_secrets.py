from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

SENSITIVE_SETTING_KEYS = {
    "mexc_api_key": "MEXC_API_KEY",
    "mexc_api_secret": "MEXC_API_SECRET",
    "openai_api_key": "OPENAI_API_KEY",
}


def _secret_path() -> Path:
    raw = os.getenv("BOT_SECRETS_PATH") or os.getenv("SECRETS_PATH")
    if raw:
        return Path(raw)
    try:
        from config import DB_PATH
        base = Path(DB_PATH)
        if base.name:
            return base.with_suffix(base.suffix + ".secrets.json")
    except Exception:
        pass
    return Path("bot_secrets.json")


def load_secret_backup() -> Dict[str, str]:
    path = _secret_path()
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        return {str(k): str(v) for k, v in data.items() if k in SENSITIVE_SETTING_KEYS and str(v or "").strip()}
    except Exception:
        return {}


def save_secret_backup(values: Dict[str, Any]) -> None:
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
        pass


def clear_secret_backup(keys: list[str] | tuple[str, ...] | set[str]) -> None:
    current = load_secret_backup()
    for k in keys:
        current.pop(str(k), None)
    try:
        path = _secret_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception:
        pass


def apply_secret_backup_to_env() -> None:
    data = load_secret_backup()
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        val = str(data.get(setting_key) or "").strip()
        if val and not os.getenv(env_key):
            os.environ[env_key] = val


def secret_value(settings: Dict[str, Any] | None, setting_key: str, env_key: str) -> str:
    # Priority: Telegram/SQLite settings -> Railway env -> backup file.
    val = str((settings or {}).get(setting_key) or "").strip()
    if val:
        return val
    val = str(os.getenv(env_key, "") or "").strip()
    if val:
        return val
    return str(load_secret_backup().get(setting_key) or "").strip()


def merge_secrets_into_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(settings or {})
    backup = load_secret_backup()
    for setting_key, env_key in SENSITIVE_SETTING_KEYS.items():
        if not str(out.get(setting_key) or "").strip():
            env_val = str(os.getenv(env_key, "") or "").strip()
            if env_val:
                out[setting_key] = env_val
            elif backup.get(setting_key):
                out[setting_key] = backup[setting_key]
    return out


def mask_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "missing"
    if len(value) <= 8:
        return "saved"
    return f"{value[:4]}...{value[-4:]}"
