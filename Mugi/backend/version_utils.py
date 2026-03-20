import json
import os
from typing import Optional

import PluginUtils

from config import PLUGIN_VERSION_RESET


logger = PluginUtils.Logger()

DEFAULT_PLUGIN_VERSION = PLUGIN_VERSION_RESET
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_MARKER_FILENAME = 'plugin_version.txt'


def _resolve_marker_path(path: Optional[str] = None) -> str:
    if path:
        return path
    return os.path.join(_BACKEND_DIR, _DEFAULT_MARKER_FILENAME)


def normalize_version(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = str(value)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def ensure_version_marker(path: Optional[str] = None, default: str = DEFAULT_PLUGIN_VERSION) -> str:
    marker_path = _resolve_marker_path(path)
    if not os.path.exists(marker_path):
        try:
            os.makedirs(os.path.dirname(marker_path), exist_ok=True)
            with open(marker_path, 'w', encoding='utf-8') as handle:
                handle.write(default)
        except Exception as error:
            logger.error(f"version_utils: failed to create version marker {marker_path}: {error}")
            return default
        return default
    return read_version_marker(marker_path, default)


def read_version_marker(path: Optional[str] = None, default: str = DEFAULT_PLUGIN_VERSION) -> str:
    marker_path = _resolve_marker_path(path)
    try:
        with open(marker_path, 'r', encoding='utf-8') as handle:
            value = handle.read().strip()
            return value or default
    except FileNotFoundError:
        return default
    except Exception as error:
        logger.warn(f"version_utils: failed to read version marker {marker_path}: {error}")
        return default


def write_version_marker(path: Optional[str], version: str) -> None:
    marker_path = _resolve_marker_path(path)
    normalized = normalize_version(version) or DEFAULT_PLUGIN_VERSION
    try:
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, 'w', encoding='utf-8') as handle:
            handle.write(normalized)
    except Exception as error:
        logger.error(f"version_utils: failed to write version marker {marker_path}: {error}")


def reset_plugin_manifest_version(plugin_dir: str, version: str = DEFAULT_PLUGIN_VERSION) -> None:
    if not plugin_dir:
        return

    manifest_path = os.path.join(plugin_dir, 'plugin.json')
    normalized = normalize_version(version) or DEFAULT_PLUGIN_VERSION
    try:
        if not os.path.exists(manifest_path):
            return

        with open(manifest_path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            data = {}

        data['version'] = normalized

        with open(manifest_path, 'w', encoding='utf-8') as handle:
            json.dump(data, handle, indent=2)
            handle.write('\n')
    except Exception as error:
        logger.warn(f"version_utils: failed to reset plugin manifest version: {error}")


def get_version_marker_path(base_path: Optional[str] = None) -> str:
    if base_path:
        return os.path.join(base_path, _DEFAULT_MARKER_FILENAME)
    return _resolve_marker_path()


def read_plugin_manifest_version(plugin_dir: Optional[str], default: str = DEFAULT_PLUGIN_VERSION) -> str:
    if not plugin_dir:
        return default

    manifest_path = os.path.join(plugin_dir, 'plugin.json')
    try:
        with open(manifest_path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return default
    except Exception as error:
        logger.warn(f"version_utils: failed to read plugin manifest version: {error}")
        return default

    if isinstance(data, dict):
        normalized = normalize_version(data.get('version'))
        if normalized:
            return normalized

    return default
