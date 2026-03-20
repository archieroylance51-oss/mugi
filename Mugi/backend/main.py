import Millennium
import PluginUtils
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from typing import Any, Dict, Optional, Tuple

from http_client import close_global_client, get_global_client, clear_global_client_headers
from api_manager import APIManager
from manilua import maniluaManager
from steam_utils import has_lua_for_app, list_lua_apps
from config import (
    API_BASE_URL,
    API_KEY_PREFIX,
    API_KEY_ALLOWED_PREFIXES,
    VERSION,
    DEFAULT_API_KEY,
    PLUGIN_UPDATE_URL,
)
from version_utils import (
    DEFAULT_PLUGIN_VERSION,
    ensure_version_marker,
    get_version_marker_path,
    normalize_version,
    read_plugin_manifest_version,
    read_version_marker,
    write_version_marker,
)

logger = PluginUtils.Logger()

class PluginUpdateError(Exception):
    """Raised when the plugin cannot be updated to the required version."""


_PLUGIN_UPDATE_DISABLED_MESSAGE = "you can't use this outdated version anymore"
_WRONG_KEY_MESSAGE = "Wrong Key"


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if not lowered:
            return None
        if lowered in {"1", "true", "yes", "on", "enabled"}:
            return True
        if lowered in {"0", "false", "no", "off", "disabled"}:
            return False
    return None


def _is_auto_update_enabled(payload: Dict[str, Any]) -> bool:
    keys = (
        "autoUpdateEnabled",
        "auto_update_enabled",
        "autoUpdate",
        "auto_update",
        "allowAutoUpdate",
        "allow_auto_update",
    )
    for key in keys:
        if key in payload:
            parsed = _normalize_bool(payload.get(key))
            if parsed is not None:
                return parsed
    return True


def _process_plugin_update_directive(
    payload: Dict[str, Any], *, allow_auto_update: bool = True
) -> Tuple[bool, Optional[Dict[str, Any]], bool]:
    if not isinstance(payload, dict):
        return False, None, False

    requires_update = False
    update_directive_requested = False
    for key in ("requiresPluginUpdate", "updateRequired"):
        normalized = _normalize_bool(payload.get(key))
        if normalized:
            requires_update = True
            update_directive_requested = True
            break

    required_version = normalize_version(payload.get("requiredVersion")) or DEFAULT_PLUGIN_VERSION
    download_url = payload.get("downloadUrl")
    plugin_version = normalize_version(payload.get("pluginVersion"))
    auto_update_enabled = _is_auto_update_enabled(payload)
    code = payload.get("code") if isinstance(payload.get("code"), str) else "plugin_update_required"
    manifest_version = (
        plugin.get_manifest_version()
        if plugin is not None
        else DEFAULT_PLUGIN_VERSION
    )

    manifest_mismatch = manifest_version != required_version
    if manifest_mismatch:
        requires_update = True

    if not requires_update:
        return False, None, False

    download_hint = (download_url or '').strip() or PLUGIN_UPDATE_URL

    if auto_update_enabled and allow_auto_update and plugin is not None:
        try:
            logger.log(
                f"Server requires plugin version {required_version}; attempting automatic update"
            )
            updated = plugin.ensure_plugin_version(required_version, download_url)
            manifest_version = plugin.get_manifest_version()
            manifest_mismatch = manifest_version != required_version
            if not manifest_mismatch:
                if updated:
                    response: Dict[str, Any] = {
                        "success": False,
                        "error": "Plugin update installed. Please restart Steam to finish applying the update.",
                        "requiresPluginUpdate": True,
                        "requiredVersion": required_version,
                        "installedVersion": manifest_version,
                        "downloadUrl": download_hint,
                        "code": "plugin_restart_required",
                        "autoUpdateEnabled": True,
                        "restartRequired": True,
                    }
                    if plugin_version:
                        response["pluginVersion"] = plugin_version
                    return True, response, True
                if not update_directive_requested:
                    return False, None, False
        except PluginUpdateError as error:
            manifest_version = plugin.get_manifest_version()
            response = {
                "success": False,
                "error": f"Plugin update failed: {error}",
                "requiresPluginUpdate": True,
                "requiredVersion": required_version,
                "installedVersion": manifest_version,
                "downloadUrl": download_hint,
                "code": "plugin_update_failed",
                "autoUpdateEnabled": auto_update_enabled,
                "manualUpdateRequired": True,
            }
            if plugin_version:
                response["pluginVersion"] = plugin_version
            return True, response, False
        except Exception as error:
            logger.error(f"Plugin update failed: {error}")
            manifest_version = plugin.get_manifest_version()
            response = {
                "success": False,
                "error": "Plugin update is required but could not be completed automatically. Please download the latest plugin and install it manually.",
                "requiresPluginUpdate": True,
                "requiredVersion": required_version,
                "installedVersion": manifest_version,
                "downloadUrl": download_hint,
                "code": "plugin_update_failed",
                "autoUpdateEnabled": auto_update_enabled,
                "manualUpdateRequired": True,
            }
            if plugin_version:
                response["pluginVersion"] = plugin_version
            return True, response, False

    if not manifest_mismatch and update_directive_requested and auto_update_enabled:
        # Server requested an update but the plugin already satisfies the requirement.
        return False, None, False

    if manifest_mismatch:
        if manifest_version:
            message = (
                f"Plugin update is required. Installed version {manifest_version} does not match required version {required_version}."
            )
        else:
            message = "Plugin update is required but the installed version could not be determined."
    elif auto_update_enabled and not allow_auto_update:
        message = "Plugin update is required but cannot be completed during this action. Please restart Steam and try again."
    elif auto_update_enabled:
        message = "Plugin update is required. Please restart Steam to install the latest version."
    else:
        message = _PLUGIN_UPDATE_DISABLED_MESSAGE

    response = {
        "success": False,
        "error": message,
        "requiresPluginUpdate": True,
        "requiredVersion": required_version,
        "installedVersion": manifest_version,
        "downloadUrl": download_hint,
        "code": code,
        "autoUpdateEnabled": auto_update_enabled,
        "manualUpdateRequired": manifest_mismatch,
    }
    if plugin_version:
        response["pluginVersion"] = plugin_version

    return True, response, False

def json_response(data: dict) -> str:
    return json.dumps(data)

def success_response(**kwargs) -> str:
    return json_response({'success': True, **kwargs})

def error_response(error: str, **kwargs) -> str:
    return json_response({'success': False, 'error': error, **kwargs})

def GetPluginDir():
    current_file = os.path.realpath(__file__)

    if current_file.endswith('/main.py/main.py') or current_file.endswith('\\main.py\\main.py'):
        current_file = current_file[:-8]
    elif current_file.endswith('/main.py') or current_file.endswith('\\main.py'):
        current_file = current_file[:-8]

    if current_file.endswith('main.py'):
        backend_dir = os.path.dirname(current_file)
    else:
        backend_dir = current_file

    plugin_dir = os.path.dirname(backend_dir)

    return plugin_dir

class Plugin:
    def __init__(self):
        self.plugin_dir = None
        self.backend_path = None
        self.api_manager = None
        self.manilua_manager = None
        self._api_key = None
        self._online_key = None
        self._injected = False
        self._version_marker_path = None
        self._cached_manifest_version: Optional[str] = None

    def _load_api_key(self):
        api_key_file = os.path.join(self.backend_path, 'api_key.txt')
        try:
            if os.path.exists(api_key_file):
                with open(api_key_file, 'r', encoding='utf-8') as f:
                    self._api_key = f.read().strip()
                if not self._api_key:
                    logger.log("API key file is empty")
        except Exception as e:
            logger.error(f"Failed to load API key: {e}")

        if (not self._api_key or not isinstance(self._api_key, str) or not self._api_key.strip()) and DEFAULT_API_KEY:
            default_key = DEFAULT_API_KEY.strip()
            if default_key:
                self._api_key = default_key
                logger.log("Using default API key from configuration")

    def _save_api_key(self, api_key: str):
        api_key_file = os.path.join(self.backend_path, 'api_key.txt')
        try:
            with open(api_key_file, 'w', encoding='utf-8') as f:
                f.write(api_key)
            self._api_key = api_key
        except Exception as e:
            logger.error(f"Failed to save API key: {e}")

    def get_api_key(self):
        return self._api_key

    def has_api_key(self):
        return self._api_key is not None and self._api_key.strip() != ""

    def _load_online_key(self):
        key_file = os.path.join(self.backend_path, 'online_key.txt')
        try:
            if os.path.exists(key_file):
                with open(key_file, 'r', encoding='utf-8') as f:
                    value = f.read().strip()
                    if value:
                        self._online_key = value
                    else:
                        self._online_key = None
            else:
                self._online_key = None
        except Exception as e:
            logger.error(f"Failed to load online key: {e}")
            self._online_key = None

    def _save_online_key(self, key):
        key_file = os.path.join(self.backend_path, 'online_key.txt')
        try:
            if key and isinstance(key, str) and key.strip():
                normalized = key.strip()
                with open(key_file, 'w', encoding='utf-8') as f:
                    f.write(normalized)
                self._online_key = normalized
            else:
                if os.path.exists(key_file):
                    os.remove(key_file)
                self._online_key = None
        except Exception as e:
            logger.error(f"Failed to persist online key: {e}")

    def clear_online_key(self):
        self._save_online_key(None)
        if self.manilua_manager:
            self.manilua_manager.clear_online_key()

    def _validate_online_key(self):
        if not self.manilua_manager or not self.has_online_key():
            return

        try:
            verification = self.manilua_manager.verify_online_key(self._online_key, clear_on_banned=True)
            update_attempted = False
            while True:
                handled, payload, updated = _process_plugin_update_directive(
                    verification,
                    allow_auto_update=not update_attempted,
                )
                if not handled:
                    break
                if payload:
                    logger.warn(payload.get('error') or 'Plugin update required')
                    return
                if updated and not update_attempted:
                    update_attempted = True
                    verification = self.manilua_manager.verify_online_key(self._online_key, clear_on_banned=True)
                    continue
                break

            # Auto-update key if server suggests a new one (upgrade/downgrade)
            if verification.get('success') and verification.get('newKey'):
                new_key = verification.get('newKey')
                if isinstance(new_key, str) and new_key.strip():
                    logger.log(f"Auto-updating online key to: {new_key[:8]}...")
                    self._save_online_key(new_key)
                    self.manilua_manager.set_online_key(new_key)
                    # Re-verify with the new key to ensure state consistency
                    self._validate_online_key()
                    return

            if not verification.get('success'):
                # Check for fallback key (e.g. expired premium -> free)
                if verification.get('fallbackKey'):
                     fallback_key = verification.get('fallbackKey')
                     if isinstance(fallback_key, str) and fallback_key.strip():
                         logger.log(f"Falling back to key: {fallback_key[:8]}...")
                         self._save_online_key(fallback_key)
                         self.manilua_manager.set_online_key(fallback_key)
                         self._validate_online_key()
                         return

                if verification.get('banned'):
                    logger.warn('Stored online access key has been banned and was cleared')
                else:
                    logger.warn(f"Stored online access key validation failed: {verification.get('error') or 'Unknown error'}")
        except Exception as error:
            logger.warn(f"Failed to validate stored online access key: {error}")

    def get_online_key(self):
        return self._online_key

    def has_online_key(self):
        return bool(self._online_key and self._online_key.strip())

    def _inject_webkit_files(self):
        if self._injected:
            return

        try:
            dist_dir = os.path.join(self.plugin_dir, '.millennium', 'Dist')
            js_file_path = os.path.join(dist_dir, 'index.js')

            if not os.path.exists(js_file_path):
                alt_js_file = os.path.join(dist_dir, 'frontend', 'index.js')
                if os.path.exists(alt_js_file):
                    js_file_path = alt_js_file

            if os.path.exists(js_file_path):
                Millennium.add_browser_js(js_file_path)
                self._injected = True
            else:
                logger.error(f"Bundle not found at {js_file_path}")
        except Exception as e:
            logger.error(f'Failed to inject: {e}')

    def _front_end_loaded(self):
        logger.log(f"v{VERSION} ready")

    def _load(self):
        global plugin
        plugin = self

        logger.log(f"backend loading (v{VERSION})")

        self.plugin_dir = GetPluginDir()
        self.backend_path = os.path.join(self.plugin_dir, 'backend')
        self._version_marker_path = get_version_marker_path(self.backend_path)
        ensure_version_marker(self._version_marker_path, DEFAULT_PLUGIN_VERSION)
        self.api_manager = APIManager(self.backend_path)
        self.manilua_manager = maniluaManager(self.backend_path, self.api_manager, self.clear_online_key)
        self._load_api_key()
        self._load_online_key()

        if self.has_api_key() and isinstance(self._api_key, str) and self._api_key.strip() != "":
            self.api_manager.set_api_key(self._api_key)
            self.manilua_manager.set_api_key(self._api_key)
        else:
            logger.log("backend initialized without API key")

        if self.has_online_key():
            self.manilua_manager.set_online_key(self._online_key)
            self._validate_online_key()

        if not self.has_online_key():
            self.manilua_manager.clear_online_key()

        self._cached_manifest_version = None

        self._inject_webkit_files()
        Millennium.ready()
        logger.log("backend ready")

    def _unload(self):
        logger.log("Unloading manilua plugin")
        close_global_client()

    def _get_required_version_marker(self) -> str:
        if not self._version_marker_path:
            return DEFAULT_PLUGIN_VERSION
        return read_version_marker(self._version_marker_path, DEFAULT_PLUGIN_VERSION)

    def get_manifest_version(self) -> str:
        if self._cached_manifest_version is not None:
            return self._cached_manifest_version

        version = read_plugin_manifest_version(self.plugin_dir, DEFAULT_PLUGIN_VERSION)
        self._cached_manifest_version = version
        return version

    def _refresh_manifest_version_cache(self) -> None:
        self._cached_manifest_version = None

    def _download_and_install_plugin(self, download_url: str) -> None:
        url = (download_url or '').strip() or PLUGIN_UPDATE_URL

        client = get_global_client()
        if not client:
            raise Exception('HTTP client unavailable')

        logger.log(f"Downloading latest plugin package from {url}")
        response = client.get_binary(url)
        if not response.get('success'):
            raise Exception(response.get('error') or 'Failed to download plugin update')

        data = response.get('data')
        if not isinstance(data, (bytes, bytearray)):
            raise Exception('Plugin update response did not contain binary data')

        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as handle:
                handle.write(data)
                temp_file = handle.name

            if not temp_file:
                raise Exception('Failed to persist plugin archive to disk')

            with zipfile.ZipFile(temp_file, 'r') as archive:
                members = archive.infolist()
                plugin_root = os.path.basename(os.path.normpath(self.plugin_dir))
                real_plugin_dir = os.path.realpath(self.plugin_dir)

                for member in members:
                    name = member.filename.replace('\\', '/')
                    # Ignore empty names that can appear for directory entries.
                    if not name.strip():
                        continue

                    parts = [segment for segment in name.split('/') if segment and segment not in {'.', '..'}]
                    if not parts:
                        continue

                    if plugin_root and parts[0].strip().lower() == plugin_root.lower():
                        parts = parts[1:]
                        if not parts:
                            continue

                    destination_path = os.path.join(self.plugin_dir, *parts)
                    destination_root = os.path.commonpath([real_plugin_dir, os.path.realpath(destination_path)])
                    if destination_root != real_plugin_dir:
                        raise Exception('Unsafe path detected in plugin archive')

                    if member.is_dir():
                        os.makedirs(destination_path, exist_ok=True)
                        continue

                    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                    with archive.open(member, 'r') as source, open(destination_path, 'wb') as target:
                        shutil.copyfileobj(source, target)
        finally:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass

        self._refresh_manifest_version_cache()
        logger.log('Plugin files updated successfully')

    def ensure_plugin_version(self, required_version: str, download_url: str = None) -> bool:
        normalized = normalize_version(required_version)
        if not normalized:
            return False

        current_marker = self._get_required_version_marker()
        current_manifest = self.get_manifest_version()

        download_required = current_manifest != normalized
        updated = False

        if download_required:
            try:
                self._download_and_install_plugin(download_url or PLUGIN_UPDATE_URL)
                updated = True
            except Exception as error:
                raise PluginUpdateError(str(error)) from error

        manifest_version = self.get_manifest_version()
        if manifest_version != normalized:
            raise PluginUpdateError(
                f'Installed plugin version {manifest_version or "unknown"} does not match required version {normalized}'
            )

        if current_marker != normalized:
            write_version_marker(self._version_marker_path, normalized)
            updated = True

        if updated:
            clear_global_client_headers()
            logger.log(f'Plugin version synchronized to {normalized}')

        return updated

plugin = None


def _check_online_key() -> Tuple[bool, Dict[str, Any]]:
    if plugin is None:
        return False, {'success': False, 'error': 'Plugin is not ready'}

    if not getattr(plugin, 'manilua_manager', None):
        return False, {'success': False, 'error': 'Online services are unavailable'}

    if not plugin.has_online_key():
        return False, {
            'success': False,
            'error': 'Online access key is not configured. Please set up your key.',
            'requiresOnlineKey': True,
            'code': 'missing',
        }

    key = plugin.get_online_key()
    if not key:
        return False, {
            'success': False,
            'error': 'Online access key is not configured. Please set up your key.',
            'requiresOnlineKey': True,
            'code': 'missing',
        }

    update_attempted = False
    verification = plugin.manilua_manager.verify_online_key(key, clear_on_banned=True)

    while True:
        handled, payload, updated = _process_plugin_update_directive(
            verification,
            allow_auto_update=not update_attempted,
        )
        if not handled:
            break
        if payload:
            return False, payload
        if updated and not update_attempted:
            update_attempted = True
            verification = plugin.manilua_manager.verify_online_key(key, clear_on_banned=True)
            continue
        break

    if not isinstance(verification, dict):
        logger.error('[Frontend] Online key validation failed: malformed verification response')
        return False, {
            'success': False,
            'error': 'Failed to verify online access key',
            'requiresOnlineKey': True,
        }

    # Auto-update key if server suggests a new one (upgrade/downgrade)
    if verification.get('success') and verification.get('newKey'):
        new_key = verification.get('newKey')
        if isinstance(new_key, str) and new_key.strip():
            logger.log(f"Auto-updating online key to: {new_key[:8]}...")
            plugin._save_online_key(new_key)
            plugin.manilua_manager.set_online_key(new_key)
            # Re-verify with the new key to ensure state consistency
            return _check_online_key()

    handled, payload, _ = _process_plugin_update_directive(verification, allow_auto_update=False)
    if handled and payload:
        return False, payload

    if not verification.get('success'):
        # Check for fallback key (e.g. expired premium -> free)
        if verification.get('fallbackKey'):
             fallback_key = verification.get('fallbackKey')
             if isinstance(fallback_key, str) and fallback_key.strip():
                 logger.log(f"Falling back to key: {fallback_key[:8]}...")
                 plugin._save_online_key(fallback_key)
                 plugin.manilua_manager.set_online_key(fallback_key)
                 return _check_online_key()

        payload = {
            'success': False,
            'error': verification.get('error') or 'Online access key rejected',
            'requiresOnlineKey': True,
        }

        requires_update = _normalize_bool(verification.get('requiresPluginUpdate')) or _normalize_bool(
            verification.get('updateRequired')
        )

        error_code = verification.get('code')
        if isinstance(error_code, str):
            payload['code'] = error_code

        connection_error = _normalize_bool(verification.get('connectionError'))
        key_rejected = _normalize_bool(verification.get('keyRejected'))

        if _normalize_bool(verification.get('hwidMismatch')):
            payload.setdefault('code', 'hwid_mismatch')

        if verification.get('banned'):
            payload['banned'] = True
            payload.setdefault('code', 'online_key_banned')
        elif not requires_update:
            if key_rejected and not connection_error:
                payload['error'] = _WRONG_KEY_MESSAGE
                payload.setdefault('code', 'wrong_key')
            elif connection_error:
                payload.setdefault('code', 'connection_error')

        log_code = payload.get('code', 'unknown')
        log_reason = payload.get('error') or 'Online access key rejected'
        context_bits = []
        if payload.get('banned'):
            context_bits.append('banned')
        if _normalize_bool(verification.get('hwidMismatch')):
            context_bits.append('hwid_mismatch')
        if requires_update:
            context_bits.append('update_required')
        context = f" ({', '.join(context_bits)})" if context_bits else ''
        logger.error(f"[Frontend] Online key validation failed{context} (code={log_code}): {log_reason}")

        return False, payload

    required_version = verification.get('requiredVersion')
    download_url = verification.get('downloadUrl')
    if plugin is not None and _normalize_bool(verification.get('updateRequired')):
        handled, payload, _ = _process_plugin_update_directive(
            verification,
            allow_auto_update=not update_attempted,
        )
        if payload:
            return False, payload

    return True, {'record': verification.get('record')}

def _fetch_online_key_info() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if plugin is None:
        return None, 'Plugin is not ready'

    key = plugin.get_online_key() or ''
    if not key:
        return None, 'Online access key is not configured'

    client = get_global_client()
    if not client:
        return None, 'HTTP client unavailable'

    response = client.get(
        f'{API_BASE_URL}/key/info',
        extra_headers={'X-Online-Key': key},
    )
    if not response.get('success'):
        return None, response.get('error') or 'Failed to load key info'

    payload = response.get('data')
    if isinstance(payload, dict) and payload.get('success'):
        return payload, None
    if isinstance(payload, dict):
        return None, payload.get('error') or 'Failed to load key info'
    return None, 'Invalid response from key info endpoint'

def _is_premium_online_key() -> Tuple[bool, str]:
    info, error = _fetch_online_key_info()
    if error:
        return False, error

    key_type = str(info.get('keyType') or '').lower()
    if key_type == 'premium':
        return True, ''

    key_value = str(info.get('key') or '').lower()
    if key_value.startswith('premium_'):
        return True, ''

    api_key = plugin.get_api_key() if plugin is not None else ''
    if isinstance(api_key, str) and api_key.lower().startswith('premium_'):
        return True, ''

    return False, 'Premium key required'

def _run_powershell_script(script_path: str, env_overrides: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
    if os.name != 'nt':
        return False, 'PowerShell is only supported on Windows'

    if not script_path or not os.path.exists(script_path):
        return False, 'Activation script was not found'

    command = [
        'powershell',
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        script_path,
    ]

    env = os.environ.copy()
    if env_overrides:
        for key, value in env_overrides.items():
            if isinstance(value, str) and value:
                env[key] = value

    startupinfo = None
    creationflags = 0
    if os.name == 'nt':  # pragma: no cover - Windows-specific configuration
        try:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= getattr(subprocess, 'STARTF_USESHOWWINDOW', 0)
        except AttributeError:
            startupinfo = None
        creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
            startupinfo=startupinfo,
            creationflags=creationflags,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, 'Activation script timed out'
    except Exception as error:
        return False, str(error)

    if completed.returncode != 0:
        output = (completed.stderr or completed.stdout or '').strip()
        return False, output or f'Activation script failed (exit code {completed.returncode})'

    return True, ''

def get_plugin():
    return plugin

class Logger:
    @staticmethod
    def log(message: str) -> str:
        logger.log(f"[Frontend] {message}")
        return success_response()

def hasluaForApp(appid: int) -> str:
    try:
        exists = has_lua_for_app(appid)
        return success_response(exists=exists)
    except Exception as e:
        logger.error(f'hasluaForApp failed for {appid}: {e}')
        return error_response(str(e))

def isMassiveOnlineApp(appid: int) -> str:
    try:
        result = plugin.manilua_manager.is_massive_online_app(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'isMassiveOnlineApp failed for {appid}: {e}')
        return error_response(str(e))

def isDGameApp(appid: int) -> str:
    try:
        result = plugin.manilua_manager.is_dgame_app(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'isDGameApp failed for {appid}: {e}')
        return error_response(str(e))

def requestDGameToken(appid: int) -> str:
    try:
        result = plugin.manilua_manager.request_dgame_token(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'requestDGameToken failed for {appid}: {e}')
        return error_response(str(e))

def addViamanilua(appid: int) -> str:
    try:
        if not plugin.has_api_key():
            return error_response('No API key configured. Please set an API key first.', requiresNewKey=True)

        valid, payload = _check_online_key()
        if not valid:
            return json_response(payload)

        endpoints = plugin.api_manager.get_download_endpoints()
        result = plugin.manilua_manager.add_via_lua(appid, endpoints)
        return json_response(result)
    except Exception as e:
        logger.error(f'addViamanilua failed for {appid}: {e}')
        return error_response(str(e))


def RefreshSteamClient() -> str:
    try:
        refreshed = False
        attempted = False

        def try_module(module) -> None:
            nonlocal refreshed, attempted

            if refreshed:
                return

            method_candidates = [
                'refresh_steam',
                'refresh_client',
                'refresh',
                'reload_client',
                'reload_steam',
                'reload',
                'restart_steam',
                'restart',
            ]

            module_name = getattr(module, '__name__', module.__class__.__name__)

            for method_name in method_candidates:
                if refreshed:
                    break

                candidate = getattr(module, method_name, None)
                if not callable(candidate):
                    continue

                attempted = True

                try:
                    candidate()
                    refreshed = True
                    logger.log(f'RefreshSteamClient: called {module_name}.{method_name}')
                except Exception as error:
                    logger.warn(
                        f'RefreshSteamClient: {module_name}.{method_name} failed: {error}'
                    )

        try_module(Millennium)
        if not refreshed:
            try_module(PluginUtils)

        return success_response(refreshed=refreshed, attempted=attempted)
    except Exception as e:
        logger.error(f'RefreshSteamClient failed: {e}')
        return error_response(str(e))

def addViaOnline(appid: int) -> str:
    try:
        valid, payload = _check_online_key()
        if not valid:
            return json_response(payload)

        result = plugin.manilua_manager.add_via_online(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'addViaOnline failed for {appid}: {e}')
        return error_response(str(e))

def checkOnlineAvailability(appid: int) -> str:
    try:
        valid, payload = _check_online_key()
        if not valid:
            return json_response(payload)

        result = plugin.manilua_manager.get_online_availability(appid)
        if not isinstance(result, dict):
            result = {'success': False, 'error': 'Unexpected availability response'}
        if 'success' not in result:
            result['success'] = bool(result.get('available'))
        return json_response(result)
    except Exception as e:
        logger.error(f'checkOnlineAvailability failed for {appid}: {e}')
        return error_response(str(e))

def requestOnlineFile(appid: int, gameName: str = None) -> str:
    try:
        valid, payload = _check_online_key()
        if not valid:
            return json_response(payload)

        result = plugin.manilua_manager.request_online_file(appid, gameName)
        if not isinstance(result, dict):
            result = {'success': False, 'error': 'Unexpected request response'}
        return json_response(result)
    except Exception as e:
        logger.error(f'requestOnlineFile failed for {appid}: {e}')
        return error_response(str(e))

def GetStatus(appid: int) -> str:
    try:
        result = plugin.manilua_manager.get_download_status(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'GetStatus failed for {appid}: {e}')
        return error_response(str(e))

def GetLocalLibrary() -> str:
    try:
        apps = list_lua_apps()
        return success_response(apps=apps)
    except Exception as e:
        logger.error(f'GetLocalLibrary failed: {e}')
        return error_response(str(e))

def SetAPIKey(*args, **kwargs) -> str:
    try:
        api_key = None
        if args:
            api_key = args[0]
        elif 'api_key' in kwargs:
            api_key = kwargs['api_key']
        elif kwargs and len(kwargs) == 1:
            api_key = next(iter(kwargs.values()))

        if not api_key or not isinstance(api_key, str):
            return error_response('Invalid API key')

        allowed_prefixes = API_KEY_ALLOWED_PREFIXES or (API_KEY_PREFIX,)
        if not any(api_key.startswith(prefix) for prefix in allowed_prefixes):
            prefix_list = ', '.join(sorted(set(prefix for prefix in allowed_prefixes if prefix)))
            return error_response(
                f'Invalid API key format (must start with one of: {prefix_list})'
            )

        plugin._save_api_key(api_key)
        plugin.api_manager.set_api_key(api_key)
        plugin.manilua_manager.set_api_key(api_key)

        return success_response(message='API key configured successfully')
    except Exception as e:
        logger.error(f'SetAPIKey failed: {e}')
        return error_response(str(e))

def GetAPIKeyStatus() -> str:
    try:
        has_key = plugin.has_api_key()
        if has_key:
            api_key = plugin.get_api_key()
            if api_key is not None:
                masked_key = api_key[:12] + '...' + api_key[-4:] if len(api_key) > 16 else api_key[:8] + '...'
                is_premium = api_key.startswith('premium_')
            else:
                masked_key = ''
                is_premium = False

            return success_response(
                hasKey=True,
                maskedKey=masked_key,
                isValid=True,
                isPremium=is_premium,
                message='API key is configured'
            )
        else:
            return success_response(
                hasKey=False,
                message='No API key configured. Please set an API key from www.piracybound.com/manilua'
            )
    except Exception as e:
        logger.error(f'GetAPIKeyStatus failed: {e}')
        return error_response(str(e))

def SetOnlineKey(*args, **kwargs) -> str:
    try:
        if plugin is None:
            return error_response('Plugin is not ready')

        key_value = None
        if args:
            key_value = args[0]
        elif 'key' in kwargs:
            key_value = kwargs['key']
        elif kwargs and len(kwargs) == 1:
            key_value = next(iter(kwargs.values()))

        if not isinstance(key_value, str):
            return error_response('Invalid key value')

        normalized = key_value.strip()
        if not normalized:
            return error_response('Invalid key value')

        verification = plugin.manilua_manager.verify_online_key(normalized)
        update_attempted = False
        while True:
            handled, payload, updated = _process_plugin_update_directive(
                verification,
                allow_auto_update=not update_attempted,
            )
            if not handled:
                break
            if payload:
                return json_response(payload)
            if updated and not update_attempted:
                update_attempted = True
                verification = plugin.manilua_manager.verify_online_key(normalized)
                continue
            break

        handled, payload, _ = _process_plugin_update_directive(verification, allow_auto_update=False)
        if handled and payload:
            return json_response(payload)

        if not verification.get('success'):
            extra: Dict[str, Any] = {}
            error_code = verification.get('code')
            if error_code:
                extra['code'] = error_code

            connection_error = _normalize_bool(verification.get('connectionError'))
            key_rejected = _normalize_bool(verification.get('keyRejected'))

            if _normalize_bool(verification.get('hwidMismatch')):
                extra.setdefault('code', 'hwid_mismatch')

            requires_update = False

            if verification.get('banned'):
                extra['banned'] = True
                message = verification.get('error') or 'Key rejected'
            else:
                requires_update = _normalize_bool(verification.get('requiresPluginUpdate')) or _normalize_bool(
                    verification.get('updateRequired')
                )
                if not requires_update:
                    if key_rejected and not connection_error:
                        extra.setdefault('code', 'wrong_key')
                        message = _WRONG_KEY_MESSAGE
                    else:
                        if connection_error:
                            extra.setdefault('code', 'connection_error')
                        message = verification.get('error') or 'Key rejected'
                else:
                    message = verification.get('error') or 'Key rejected'

            log_code = extra.get('code', 'unknown')
            context_bits = []
            if extra.get('banned'):
                context_bits.append('banned')
            if _normalize_bool(verification.get('hwidMismatch')):
                context_bits.append('hwid_mismatch')
            if requires_update:
                context_bits.append('update_required')
            context = f" ({', '.join(context_bits)})" if context_bits else ''
            logger.error(f"[Frontend] Online key rejected{context} (code={log_code}): {message}")

            return error_response(message, **extra)

        plugin._save_online_key(normalized)
        plugin.manilua_manager.set_online_key(normalized)

        return success_response(message='Online access configured', record=verification.get('record'))
    except Exception as e:
        logger.error(f'SetOnlineKey failed: {e}')
        return error_response(str(e))

def ClearOnlineKey() -> str:
    try:
        if plugin is None:
            return error_response('Plugin is not ready')

        plugin.clear_online_key()
        return success_response(message='Online access key cleared')
    except Exception as e:
        logger.error(f'ClearOnlineKey failed: {e}')
        return error_response(str(e))

def GetOnlineAccessStatus() -> str:
    try:
        if plugin is None:
            return success_response(configured=False, message='Plugin is not ready')

        if plugin.has_online_key():
            key = plugin.get_online_key() or ''
            masked = key[:4] + '...' + key[-4:] if len(key) > 8 else key[:4] + '...'
            is_premium = key.startswith('premium_')
            return success_response(configured=True, maskedKey=masked, isPremium=is_premium)

        return success_response(configured=False)
    except Exception as e:
        logger.error(f'GetOnlineAccessStatus failed: {e}')
        return error_response(str(e))

def GetOnlineKeyInfo() -> str:
    try:
        if plugin is None:
            return error_response('Plugin is not ready')

        if not plugin.has_online_key():
            return success_response(configured=False, message='No online key configured')

        key = plugin.get_online_key() or ''
        client = get_global_client()

        response = client.get(
            f'{API_BASE_URL}/key/info',
            extra_headers={'X-Online-Key': key}
        )

        if not response.get('success'):
            return error_response(response.get('error') or 'Failed to load key info')

        payload = response.get('data')
        if isinstance(payload, dict):
            return json_response(payload)
        return error_response('Invalid response from key info endpoint')
    except Exception as e:
        logger.error(f'GetOnlineKeyInfo failed: {e}')
        return error_response(str(e))

def ActivateFixConnection(*args, **kwargs) -> str:
    try:
        appid = None
        game_name = None
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                appid = args[0].get('appid')
                game_name = args[0].get('gameName')
            else:
                appid = args[0]
                if len(args) > 1:
                    game_name = args[1]
        if 'appid' in kwargs:
            appid = kwargs.get('appid')
        if 'gameName' in kwargs:
            game_name = kwargs.get('gameName')

        parsed_appid = None
        if isinstance(appid, int):
            parsed_appid = appid if appid > 0 else None
        elif isinstance(appid, str):
            try:
                parsed_value = int(appid.strip())
                if parsed_value > 0:
                    parsed_appid = parsed_value
            except ValueError:
                parsed_appid = None

        valid, payload = _check_online_key()
        if not valid:
            return json_response(payload)

        is_premium, premium_error = _is_premium_online_key()
        if not is_premium:
            return error_response(premium_error or 'Premium key required', code='premium_required')

        if plugin is None or not plugin.has_online_key():
            return error_response('Online access key is not configured', code='missing')

        key = plugin.get_online_key() or ''
        client = get_global_client()
        if not client:
            return error_response('HTTP client unavailable')

        params: Dict[str, Any] = {}
        if parsed_appid is not None:
            params['appid'] = parsed_appid
        if isinstance(game_name, str) and game_name.strip():
            params['gameName'] = game_name.strip()

        response = client.get_binary(
            f'{API_BASE_URL}/premium/fix-connection/activate',
            params=params or None,
            extra_headers={'X-Online-Key': key},
        )
        if not response.get('success'):
            return error_response(response.get('error') or 'Failed to download activation script')

        data = response.get('data')
        if not isinstance(data, (bytes, bytearray)):
            return error_response('Activation script did not return binary data')

        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.ps1') as handle:
                handle.write(data)
                temp_file = handle.name

            env_overrides: Dict[str, str] = {}
            if parsed_appid is not None:
                env_overrides['MUGI_APPID'] = str(parsed_appid)
            if isinstance(game_name, str) and game_name.strip():
                env_overrides['MUGI_GAME_NAME'] = game_name.strip()

            ok, error_message = _run_powershell_script(temp_file, env_overrides)
            if not ok:
                return error_response(error_message or 'Activation failed')
        finally:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass

        return success_response(message='Activated successfully, check the downloads')
    except Exception as e:
        logger.error(f'ActivateFixConnection failed: {e}')
        return error_response(str(e))


def ValidateOnlineKey() -> str:
    try:
        valid, payload = _check_online_key()
        if valid:
            record = payload.get('record') if isinstance(payload, dict) else None
            return success_response(valid=True, record=record)
        return json_response(payload)
    except Exception as e:
        logger.error(f'ValidateOnlineKey failed: {e}')
        return error_response(str(e))

def removeViamanilua(appid: int) -> str:
    try:
        result = plugin.manilua_manager.remove_via_lua(appid)
        return json_response(result)
    except Exception as e:
        logger.error(f'removeViamanilua failed for {appid}: {e}')
        return error_response(str(e))
