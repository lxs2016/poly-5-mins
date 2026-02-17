"""
Load config from config.yaml and environment variables (env overrides).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, Union

try:
    import yaml
except ImportError:
    yaml = None

CONFIG_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = CONFIG_DIR / "data"


def load_config(config_path: Optional[Union[str, Path]] = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else CONFIG_DIR / "config.yaml"
    out: dict[str, Any] = {
        "data_dir": str(DEFAULT_DATA_DIR),
        "log_level": "INFO",
        "gamma_base_url": "https://gamma-api.polymarket.com",
        "clob_ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "storage_flush_interval": 5.0,
        "storage_flush_max_rows": 200,
    }
    if path.exists() and yaml is not None:
        try:
            with open(path, encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            out.update(loaded)
        except Exception:
            pass
    env_map = {
        "DATA_DIR": "data_dir",
        "LOG_LEVEL": "log_level",
        "GAMMA_BASE_URL": "gamma_base_url",
        "CLOB_WS_URL": "clob_ws_url",
    }
    for env_key, cfg_key in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            if cfg_key == "storage_flush_interval" or cfg_key == "storage_flush_max_rows":
                try:
                    out[cfg_key] = float(val) if "interval" in cfg_key else int(val)
                except ValueError:
                    pass
            else:
                out[cfg_key] = val
    return out
