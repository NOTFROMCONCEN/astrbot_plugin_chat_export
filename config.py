from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_UTILS_PATH = Path(__file__).resolve().parent.parent / "utils"
if str(_UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(_UTILS_PATH))

try:
    from config_utils import env_override, mask_key, mask_config_for_log
except Exception:
    env_override = None  # type: ignore[misc]
    mask_key = None  # type: ignore[misc]
    mask_config_for_log = None  # type: ignore[misc]


def load_secure_config(config: dict[str, Any]) -> None:
    if env_override is None:
        return
    overrides = {
        "embedding_api_key": "CHAT_EXPORT_EMBEDDING_API_KEY",
        "analysis_api_key": "CHAT_EXPORT_ANALYSIS_API_KEY",
        "qdrant_api_key": "CHAT_EXPORT_QDRANT_API_KEY",
        "lsky_token": "CHAT_EXPORT_LSKY_TOKEN",
    }
    for key, env_name in overrides.items():
        val = env_override(config, key, env_name)
        if val:
            config[key] = val


def int_conf(config: dict[str, Any], key: str, default: int) -> int:
    try:
        return int(config.get(key, default))
    except Exception:
        return default


def float_conf(config: dict[str, Any], key: str, default: float) -> float:
    try:
        return float(config.get(key, default))
    except Exception:
        return default


def resolve_data_dir(plugin_dir: Path, config: dict[str, Any]) -> Path:
    from .utils import norm
    raw = norm(config.get("data_dir", ""))
    if raw:
        path = Path(raw)
        if path.is_absolute():
            return path
        return plugin_dir / path

    astrbot_data = Path("/AstrBot/data")
    if astrbot_data.exists():
        return astrbot_data / "plugin_data" / "astrbot_plugin_chat_export"

    return plugin_dir / "data"


def resolve_path(p: Any, base_dir: Path, default_name: str = "chat_export.db") -> Path:
    from .utils import norm
    raw = norm(p)
    if not raw:
        return base_dir / default_name
    path = Path(raw)
    if path.is_absolute():
        return path
    return base_dir / path
