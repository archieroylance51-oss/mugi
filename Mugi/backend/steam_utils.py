import os
import re
import sys
from typing import Optional, List
import Millennium
import PluginUtils

logger = PluginUtils.Logger()

if sys.platform.startswith('win'):
    try:
        import winreg
    except Exception:
        winreg = None

_steam_install_path: Optional[str] = None
_stplug_in_path_cache: Optional[str] = None
_library_paths_cache: Optional[List[str]] = None

def detect_steam_install_path() -> str:
    global _steam_install_path

    if _steam_install_path:
        return _steam_install_path

    path = None

    if sys.platform.startswith('win') and winreg is not None:
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Valve\Steam") as key:
                path, _ = winreg.QueryValueEx(key, 'SteamPath')
        except Exception:
            path = None

    if not path:
        try:
            path = Millennium.steam_path()
        except Exception:
            path = None

    _steam_install_path = path
    return _steam_install_path or ''


def _normalize_path(value: str) -> str:
    normalized = value.replace('\\\\', '\\')
    return os.path.normpath(normalized)


def get_steam_library_paths(force_refresh: bool = False) -> List[str]:
    global _library_paths_cache

    if _library_paths_cache is not None and not force_refresh:
        return list(_library_paths_cache)

    libraries: List[str] = []

    base_path = detect_steam_install_path()
    if base_path:
        libraries.append(os.path.normpath(base_path))

    if base_path:
        library_file = os.path.join(base_path, 'steamapps', 'libraryfolders.vdf')
    else:
        library_file = ''

    if library_file and os.path.exists(library_file):
        try:
            with open(library_file, 'r', encoding='utf-8', errors='ignore') as handle:
                for line in handle:
                    tokens = re.findall(r'"([^\"]+)"', line)
                    if len(tokens) >= 2 and tokens[0].lower() == 'path':
                        try:
                            libraries.append(_normalize_path(tokens[1]))
                        except Exception:
                            continue
        except Exception as error:
            logger.warn(f'manilua (steam_utils): Failed to parse libraryfolders.vdf: {error}')

    unique_libraries = []
    seen = set()
    for path_value in libraries:
        key = path_value.lower()
        if key not in seen:
            seen.add(key)
            unique_libraries.append(path_value)

    _library_paths_cache = unique_libraries
    return list(unique_libraries)


def get_app_install_path(appid: int) -> str:
    try:
        app_id_int = int(appid)
    except (ValueError, TypeError):
        return ''

    library_paths = get_steam_library_paths()

    for library in library_paths:
        steamapps_path = os.path.join(library, 'steamapps')
        manifest_path = os.path.join(steamapps_path, f'appmanifest_{app_id_int}.acf')

        if not os.path.exists(manifest_path):
            continue

        installdir = None
        try:
            with open(manifest_path, 'r', encoding='utf-8', errors='ignore') as manifest_file:
                for line in manifest_file:
                    tokens = re.findall(r'"([^\"]+)"', line)
                    if len(tokens) >= 2 and tokens[0].lower() == 'installdir':
                        installdir = tokens[1]
                        break
        except Exception as error:
            logger.warn(f'manilua (steam_utils): Failed to read manifest for app {app_id_int}: {error}')
            installdir = None

        if not installdir:
            continue

        try:
            install_dir_name = installdir.strip()
            if not install_dir_name:
                continue
            install_path = os.path.join(steamapps_path, 'common', install_dir_name)
            if os.path.exists(install_path):
                return install_path
            return install_path
        except Exception as error:
            logger.warn(f'manilua (steam_utils): Error resolving path for app {app_id_int}: {error}')
            continue

    return ''

def get_steam_config_path() -> str:
    steam_path = detect_steam_install_path()
    if not steam_path:
        raise RuntimeError("Steam installation path not found")
    return os.path.join(steam_path, 'config')

def get_stplug_in_path() -> str:
    global _stplug_in_path_cache

    if _stplug_in_path_cache:
        return _stplug_in_path_cache

    config_path = get_steam_config_path()
    stplug_path = os.path.join(config_path, 'stplug-in')
    os.makedirs(stplug_path, exist_ok=True)
    _stplug_in_path_cache = stplug_path
    return stplug_path

def get_depotcache_path() -> str:
    config_path = get_steam_config_path()
    depotcache_path = os.path.join(config_path, 'depotcache')
    os.makedirs(depotcache_path, exist_ok=True)
    return depotcache_path

def has_lua_for_app(appid: int) -> bool:
    try:
        base_path = detect_steam_install_path()
        if not base_path:
            return False

        stplug_path = os.path.join(base_path, 'config', 'stplug-in')
        lua_file = os.path.join(stplug_path, f'{appid}.lua')
        disabled_file = os.path.join(stplug_path, f'{appid}.lua.disabled')

        exists = os.path.exists(lua_file) or os.path.exists(disabled_file)
        return exists

    except Exception as e:
        logger.error(f'manilua (steam_utils): Error checking Lua scripts for app {appid}: {e}')
        return False

def list_lua_apps() -> list:
    try:
        base_path = detect_steam_install_path()
        if not base_path:
            return []

        stplug_path = os.path.join(base_path, 'config', 'stplug-in')
        if not os.path.exists(stplug_path):
            return []

        apps_mtime = {}
        for filename in os.listdir(stplug_path):
            if filename.endswith('.lua') or filename.endswith('.lua.disabled'):
                name = filename.split('.')[0]
                if not name.isdigit():
                    continue
                appid = int(name)
                path = os.path.join(stplug_path, filename)
                try:
                    mtime = os.path.getmtime(path)
                    apps_mtime[appid] = mtime
                except Exception:
                    continue

        return sorted(apps_mtime.keys(), key=lambda a: apps_mtime[a], reverse=True)

    except Exception as e:
        logger.error(f'manilua (steam_utils): list_lua_apps failed: {e}')
        return []
