import os
import zipfile
import threading
import subprocess
import json
import shutil
import time
from urllib.parse import urljoin
from typing import Dict, Any, List, Optional, Callable
import PluginUtils
from http_client import get_global_client
from steam_utils import get_stplug_in_path, get_depotcache_path, get_app_install_path
from api_manager import APIManager
from config import (
    API_BASE_URL,
    ONLINE_API_BASE_URL,
    HTTP_CHUNK_SIZE,
    DOWNLOAD_PROGRESS_UPDATE_INTERVAL,
    D_TYPE,
)

try:
    import httpx
    from httpx import HTTPStatusError
    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None
    HTTPStatusError = None
    HTTPX_AVAILABLE = False

logger = PluginUtils.Logger()


class OnlineKeyAuthorizationError(Exception):
    def __init__(self, message: str, banned: bool = False):
        super().__init__(message)
        self.banned = banned


_ONLINE_KEY_RESET_KEYWORDS = (
    'online access key',
    'online key',
    'x-online-key',
    'authorization',
    'unauthorized',
    'forbidden',
    'invalid key',
    'wrong key',
)

_ONLINE_KEY_BANNED_KEYWORDS = (
    'ban',
    'revok',
    'suspend',
    'disable',
    'blacklist',
)

_ONLINE_KEY_INVALID_CODES = {
    'invalid_key',
    'wrong_key',
    'invalid_online_key',
    'online_key_invalid',
}

class maniluaManager:
    def __init__(self, backend_path: str, api_manager: APIManager, on_online_key_invalid: Optional[Callable[[], None]] = None):
        self.backend_path = backend_path
        self.api_manager = api_manager
        self._download_state: Dict[int, Dict[str, Any]] = {}
        self._download_lock = threading.Lock()
        self._api_key = None
        self._online_key: Optional[str] = None
        self._on_online_key_invalid = on_online_key_invalid

        self._online_password = 'online-fix.me'

        self._online_password = 'online-fix.me'

    def _classify_online_key_error(
        self,
        message: Optional[str],
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        text = (message or '').strip()
        normalized = text.lower()
        code_normalized = (error_code or '').lower()

        banned = False
        if status_code == 403:
            banned = True
        if any(keyword in normalized for keyword in _ONLINE_KEY_BANNED_KEYWORDS):
            banned = True
        if any(keyword in code_normalized for keyword in _ONLINE_KEY_BANNED_KEYWORDS):
            banned = True

        if 'plugin_update_required' in code_normalized or 'update required' in normalized:
            return {
                'requires_reset': False,
                'banned': False,
                'message': text or 'Plugin update required. Please update the plugin.',
            }

        code_for_keywords = code_normalized.replace('_', ' ').replace('-', ' ')
        requires_reset = banned or status_code in (401, 403) or any(
            keyword in normalized for keyword in _ONLINE_KEY_RESET_KEYWORDS
        ) or any(
            keyword in code_for_keywords for keyword in _ONLINE_KEY_RESET_KEYWORDS
        )

        sanitized = text or 'Online access key was rejected by the server'
        if banned and not any(term in normalized for term in ('banned', 'revoked')):
            sanitized = 'Online access key has been banned or revoked. Please configure a new key.'

        return {
            'requires_reset': requires_reset,
            'banned': banned,
            'message': sanitized,
        }

    def _should_flag_key_rejected(
        self,
        error_code: Optional[str],
        classification: Dict[str, Any],
        *,
        status_code: Optional[int] = None,
        connection_error: bool = False,
    ) -> bool:
        if connection_error or classification.get('banned'):
            return False
        normalized = (error_code or '').strip().lower()
        if normalized:
            if normalized in _ONLINE_KEY_INVALID_CODES:
                return True
            if (
                'hwid' in normalized
                or 'update' in normalized
                or 'plugin' in normalized
                or 'api_key' in normalized
                or 'connection' in normalized
                or normalized in {'missing', 'required'}
            ):
                return False
            return False
        return bool(classification.get('requires_reset') or status_code in (401, 403))

    def _handle_invalid_online_key(self, banned: bool, message: Optional[str] = None) -> None:
        reason = message or 'Online access key was rejected by the server'
        if banned:
            logger.warn(f'Online access key banned or revoked: {reason}')
        else:
            logger.warn(f'Online access key rejected: {reason}')

        self.clear_online_key()
        if callable(self._on_online_key_invalid):
            try:
                self._on_online_key_invalid()
            except Exception as error:
                logger.warn(f'Failed to notify about invalid online key: {error}')

    def _build_online_api_url(self, path: str) -> str:
        base = ONLINE_API_BASE_URL.rstrip('/')
        normalized = path.lstrip('/')
        if normalized.startswith('api/'):
            return f'{base}/{normalized}'
        return f'{base}/api/{normalized}'

    def _build_plugin_api_url(self, path: str) -> str:
        base = API_BASE_URL.rstrip('/')
        normalized = path.lstrip('/')
        return f'{base}/{normalized}'

    def set_api_key(self, api_key: str):
        self._api_key = api_key

    def get_api_key(self):
        return self._api_key

    def set_online_key(self, key: Optional[str]):
        self._online_key = key.strip() if isinstance(key, str) and key.strip() else None

    def clear_online_key(self):
        self._online_key = None

    def get_online_key(self) -> Optional[str]:
        return self._online_key

    def has_online_key(self) -> bool:
        return bool(self._online_key)

    def _set_download_state(self, appid: int, update: Dict[str, Any]) -> None:
        with self._download_lock:
            state = self._download_state.get(appid, {})
            state.update(update)
            self._download_state[appid] = state

    def _get_download_state(self, appid: int) -> Dict[str, Any]:
        with self._download_lock:
            return self._download_state.get(appid, {}).copy()

    def get_download_status(self, appid: int) -> Dict[str, Any]:
        state = self._get_download_state(appid)
        return {'success': True, 'state': state}

    def verify_online_key(self, key: str, *, clear_on_banned: bool = False) -> Dict[str, Any]:
        normalized = key.strip() if isinstance(key, str) else ''
        if not normalized:
            return {'success': False, 'error': 'Missing online key value'}

        try:
            client = get_global_client()
        except Exception as error:
            logger.error(f"verify_online_key: unable to get HTTP client: {error}")
            return {
                'success': False,
                'error': 'HTTP client unavailable',
                'code': 'connection_error',
                'connectionError': True,
            }

        if not client:
            return {
                'success': False,
                'error': 'HTTP client unavailable',
                'code': 'connection_error',
                'connectionError': True,
            }

        try:
            response = client.post(
                self._build_online_api_url('plugin/online-files/authenticate'),
                data={'key': normalized}
            )
        except Exception as error:
            logger.error(f"verify_online_key: request failed: {error}")
            return {
                'success': False,
                'error': str(error),
                'code': 'connection_error',
                'connectionError': True,
            }

        error_code: Optional[str] = None
        status_code: Optional[int] = response.get('status_code') if isinstance(response, dict) else None

        if not response.get('success'):
            error_message = response.get('error') or 'Verification failed'
            data = response.get('data')
            if isinstance(data, dict):
                if isinstance(data.get('error'), str):
                    error_message = data['error']
                if isinstance(data.get('code'), str):
                    error_code = data['code']

            classification = self._classify_online_key_error(
                error_message,
                status_code=status_code,
                error_code=error_code,
            )

            if classification['banned'] and (clear_on_banned or normalized == (self._online_key or '')):
                self._handle_invalid_online_key(True, classification['message'])

            result: Dict[str, Any] = {
                'success': False,
                'error': classification['message'],
            }

            if error_code:
                result['code'] = error_code
            if classification['banned']:
                result['banned'] = True
                result.setdefault('code', 'online_key_banned')
            if classification['requires_reset']:
                connection_error = status_code is None
                key_rejected = self._should_flag_key_rejected(
                    error_code,
                    classification,
                    status_code=status_code,
                    connection_error=connection_error,
                )
                if key_rejected:
                    result['keyRejected'] = True
            if status_code is None:
                result['connectionError'] = True
                result.setdefault('code', 'connection_error')

            return result

        payload = response.get('data')
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        if isinstance(payload, dict) and payload.get('success'):
            result: Dict[str, Any] = {'success': True, 'record': payload.get('record')}

            if isinstance(payload.get('newKey'), str):
                result['newKey'] = payload['newKey']

            if isinstance(payload.get('requiredVersion'), (str, int, float)):
                result['requiredVersion'] = str(payload['requiredVersion']).strip()

            if isinstance(payload.get('pluginVersion'), (str, int, float)):
                result['pluginVersion'] = str(payload['pluginVersion']).strip()

            if 'updateRequired' in payload:
                result['updateRequired'] = bool(payload['updateRequired'])

            if isinstance(payload.get('downloadUrl'), str):
                result['downloadUrl'] = payload['downloadUrl']

            return result

        if isinstance(payload, dict) and payload.get('requiresPluginUpdate'):
            update_result: Dict[str, Any] = {
                'success': False,
                'error': payload.get('error') or 'Plugin update required',
                'requiresPluginUpdate': True,
                'updateRequired': True,
            }

            if isinstance(payload.get('requiredVersion'), (str, int, float)):
                update_result['requiredVersion'] = str(payload['requiredVersion']).strip()

            if isinstance(payload.get('pluginVersion'), (str, int, float)):
                update_result['pluginVersion'] = str(payload['pluginVersion']).strip()

            if isinstance(payload.get('downloadUrl'), str):
                update_result['downloadUrl'] = payload['downloadUrl']

            if isinstance(payload.get('code'), str):
                update_result['code'] = payload['code']

            return update_result

        error_message = 'Online key rejected'
        if isinstance(payload, dict):
            if payload.get('error'):
                error_message = str(payload.get('error'))
            if isinstance(payload.get('code'), str):
                error_code = payload['code']

        classification = self._classify_online_key_error(
            error_message,
            error_code=error_code,
        )

        if classification['banned'] and (clear_on_banned or normalized == (self._online_key or '')):
            self._handle_invalid_online_key(True, classification['message'])

        result: Dict[str, Any] = {'success': False, 'error': classification['message']}

        if isinstance(payload, dict) and payload.get('fallbackKey'):
             result['fallbackKey'] = payload.get('fallbackKey')

        if error_code:
            result['code'] = error_code
        if classification['banned']:
            result['banned'] = True
            result.setdefault('code', 'online_key_banned')
        if classification['requires_reset']:
            connection_error = status_code is None
            key_rejected = self._should_flag_key_rejected(
                error_code,
                classification,
                status_code=status_code,
                connection_error=connection_error,
            )
            if key_rejected:
                result['keyRejected'] = True

        return result

    def _download_from_manilua_backend(self, appid: int, endpoint: str = "") -> None:
        try:
            self._set_download_state(appid, {
                'status': 'Generating Lua, one moment...',
                'bytesRead': 0,
                'totalBytes': 0,
                'endpoint': endpoint
            })

            client = get_global_client()
            if not client:
                raise Exception("Failed to get HTTP client")

        except Exception as e:
            logger.error(f"Fatal error in download setup: {e}")
            self._set_download_state(appid, {
                'status': 'failed',
                'error': f'Setup failed: {str(e)}'
            })
            return

        try:
            download_url = f'{API_BASE_URL}/game/{appid}'

            api_key = self.get_api_key()
            online_key = self.get_online_key()
            if not online_key:
                raise Exception('Online key is not configured')

            params = {'appid': appid}
            if D_TYPE == 2:
                params['dtype'] = 2

            temp_zip_path = os.path.join(self.backend_path, f"temp_{appid}.zip")
            bytes_read = 0
            last_state_update_ts = 0.0

            try:
                headers = {'X-Online-Key': online_key}

                with client.stream_get(
                    download_url,
                    params=params,
                    auth_token=api_key,
                    extra_headers=headers,
                ) as resp:
                    if not resp.is_success:
                        if resp.status_code == 401:
                            raise Exception("API key authentication failed")
                        elif resp.status_code == 404:
                            raise Exception(f"Game {appid} not found")
                        elif resp.status_code == 429:
                            self._set_download_state(appid, {
                                'status': 'failed',
                                'error': 'You ran out of points, check the user panel for more details; please wait for the weekly reset or consider getting premium from https://mugi.store',
                                'endpoint': endpoint,
                            })
                            return
                        else:
                            raise Exception(f"HTTP {resp.status_code}: {resp.reason_phrase}")

                    try:
                        total = int(resp.headers.get('Content-Length', '0'))
                    except Exception as e:
                        logger.warn(f"Could not parse Content-Length header: {e}")
                        total = 0

                    content_type = resp.headers.get('content-type', '').lower()
                    if 'application/json' in content_type:
                        try:
                            raw_payload = resp.read()
                        except Exception:
                            raw_payload = b''

                        try:
                            error_text = raw_payload.decode('utf-8')
                        except Exception:
                            error_text = raw_payload.decode('utf-8', errors='ignore')

                        logger.error(f"Received JSON error response: {error_text}")

                        payload_data = None
                        try:
                            payload_data = json.loads(error_text)
                        except Exception:
                            payload_data = None

                        payload_code = ''
                        payload_message = error_text
                        if isinstance(payload_data, dict):
                            payload_code = str(payload_data.get('code') or '').strip().lower()
                            payload_message = str(
                                payload_data.get('error')
                                or payload_data.get('message')
                                or error_text
                            )

                        if resp.status_code in (401, 403) and payload_code in (
                            'invalid_key',
                            'online_key_banned',
                            'missing',
                            'hwid_mismatch',
                        ):
                            classification = self._classify_online_key_error(
                                payload_message,
                                status_code=resp.status_code,
                                error_code=payload_code,
                            )

                            state_update = {
                                'status': 'failed',
                                'error': classification['message'],
                                'requiresOnlineKey': True,
                                'endpoint': endpoint,
                            }
                            if payload_code:
                                state_update['code'] = payload_code
                            if classification['banned']:
                                state_update['onlineKeyBanned'] = True

                            self._set_download_state(appid, state_update)

                            if classification['requires_reset']:
                                self._handle_invalid_online_key(
                                    classification['banned'],
                                    classification['message'],
                                )

                            return

                        if (
                            resp.status_code == 401
                            or 'authentication' in payload_message.lower()
                            or payload_code in ('invalid_api_key', 'api_key_missing', 'api_key_required')
                        ):
                            raise Exception('API key authentication failed')

                        raise Exception(f"Server error: {payload_message}")

                    self._set_download_state(appid, {
                        'status': 'downloading',
                        'bytesRead': 0,
                        'totalBytes': total
                    })

                    with open(temp_zip_path, 'wb', buffering=HTTP_CHUNK_SIZE) as f:
                        for chunk in resp.iter_bytes(chunk_size=HTTP_CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            bytes_read += len(chunk)

                            try:
                                import time as _time
                                now_ts = _time.time()
                            except Exception as e:
                                logger.warn(f"Could not get timestamp for download progress: {e}")
                                now_ts = 0.0

                            if last_state_update_ts == 0.0 or (now_ts - last_state_update_ts) >= DOWNLOAD_PROGRESS_UPDATE_INTERVAL:
                                self._set_download_state(appid, {
                                    'status': 'downloading',
                                    'bytesRead': bytes_read,
                                    'totalBytes': total,
                                    'endpoint': endpoint
                                })
                                last_state_update_ts = now_ts

                if bytes_read <= 0:
                    raise Exception("Empty download from endpoint")

                
                self._set_download_state(appid, {
                    'status': 'processing',
                    'bytesRead': bytes_read,
                    'totalBytes': bytes_read if total == 0 else total
                })

                logger.log(f"Downloaded {bytes_read} bytes to {temp_zip_path}")

                try:
                    is_zip = zipfile.is_zipfile(temp_zip_path)
                except Exception as e:
                    logger.warn(f"Could not verify if file is ZIP for app {appid}: {e}")
                    is_zip = False

                if is_zip:
                    if D_TYPE == 2:
                        self._extract_lua_and_manifest_from_zip(appid, temp_zip_path, endpoint)
                    else:
                        self._extract_and_add_lua_from_zip(appid, temp_zip_path, endpoint)
                    if os.path.exists(temp_zip_path):
                        os.remove(temp_zip_path)
                else:
                    try:
                        target_dir = get_stplug_in_path()
                        dest_file = os.path.join(target_dir, f"{appid}.lua")

                        try:
                            with open(temp_zip_path, 'rb') as src, open(dest_file, 'wb') as dst:
                                dst.write(src.read())
                            os.remove(temp_zip_path)
                        except Exception as e:
                            logger.warn(f"Could not copy file for app {appid}: {e}")
                            raise

                        self._set_download_state(appid, {
                            'status': 'installing',
                            'installedFiles': [dest_file],
                            'installedPath': dest_file
                        })
                        logger.log(f"Installed single LUA file for app {appid}: {dest_file}")
                    except Exception as e:
                        logger.error(f"Failed to install non-zip payload for app {appid}: {e}")
                        raise

                self._set_download_state(appid, {
                    'status': 'done',
                    'success': True,
                    'api': f'manilua ({endpoint})'
                })

            except Exception as e:
                if os.path.exists(temp_zip_path):
                    try:
                        os.remove(temp_zip_path)
                    except Exception as e2:
                        logger.warn(f"Could not remove temp file on error cleanup for app {appid}: {e2}")

                error_message = str(e)
                if "authentication failed" in error_message.lower() or (HTTPX_AVAILABLE and HTTPStatusError is not None and isinstance(e, HTTPStatusError) and e.response.status_code == 401):
                    logger.error(f"API key authentication failed for app {appid}")
                    self._set_download_state(appid, {
                        'status': 'auth_failed',
                        'error': 'API key authentication failed. Please set a valid API key.',
                        'requiresNewKey': True
                    })
                    return

                self._set_download_state(appid, {
                    'status': 'failed',
                    'error': f'Download failed: {str(e)}'
                })

        except Exception as e:
            logger.error(f"Backend download failed: {str(e)}")
            self._set_download_state(appid, {
                'status': 'failed',
                'error': f'Backend error: {str(e)}'
            })

    def _install_local_lua(self, appid: int, source_path: str) -> None:
        try:
             self._set_download_state(appid, {
                'status': 'Lua found, downloading...',
                'bytesRead': 0,
                'totalBytes': os.path.getsize(source_path)
            })
             
             target_dir = get_stplug_in_path()
             dest_file = os.path.join(target_dir, f"{appid}.lua")
             
             shutil.copy2(source_path, dest_file)
             
             self._set_download_state(appid, {
                'status': 'installing',
                'installedFiles': [dest_file],
                'installedPath': dest_file
            })
             
             logger.log(f"Installed local LUA file for app {appid}: {dest_file}")
             
             self._set_download_state(appid, {
                'status': 'done',
                'success': True,
                'api': 'local-file'
            })
             
        except Exception as e:
            logger.error(f"Failed to install local file for app {appid}: {e}")
            self._set_download_state(appid, {
                'status': 'failed',
                'error': f'Local install failed: {str(e)}'
            })

    def _extract_and_add_lua_from_zip(self, appid: int, zip_path: str, endpoint: str) -> None:
        try:
            target_dir = get_stplug_in_path()
            installed_files = []

            self._set_download_state(appid, {'status': 'extracting'})
            logger.log(f"Extracting ZIP file {zip_path} to {target_dir}")

            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                file_list = zip_file.namelist()
                logger.log(f"ZIP contains {len(file_list)} files")

                lua_files = [f for f in file_list if f.lower().endswith('.lua')]

                if not lua_files:
                    logger.warn(f"No .lua files found in ZIP, extracting all files")
                    lua_files = file_list

                self._set_download_state(appid, {'status': 'installing'})

                installed_files = []

                for file_name in lua_files:
                    if file_name.endswith('/'):
                        continue

                    try:
                        file_content = zip_file.read(file_name)

                        if file_name.lower().endswith('.lua'):
                            base_name = os.path.basename(file_name)
                            dest_file = os.path.join(target_dir, base_name)
                        else:
                            file_ext = os.path.splitext(file_name)[1] or '.txt'
                            dest_file = os.path.join(target_dir, f"{appid}{file_ext}")

                        if isinstance(file_content, bytes):
                            if file_name.lower().endswith('.lua'):
                                try:
                                    decoded_content = file_content.decode('utf-8')
                                    with open(dest_file, 'w', encoding='utf-8') as out:
                                        out.write(decoded_content)
                                except UnicodeDecodeError:
                                    with open(dest_file, 'wb') as out:
                                        out.write(file_content)
                            else:
                                with open(dest_file, 'wb') as out:
                                    out.write(file_content)
                        else:
                            with open(dest_file, 'w', encoding='utf-8') as out:
                                out.write(str(file_content))

                        installed_files.append(dest_file)

                    except Exception as e:
                        logger.error(f"Failed to extract {file_name}: {e}")
                        continue

            if not installed_files:
                raise Exception("No files were successfully extracted from ZIP")

            logger.log(f"Successfully installed {len(installed_files)} files from {endpoint}")
            self._set_download_state(appid, {
                'installedFiles': installed_files,
                'installedPath': installed_files[0] if installed_files else None
            })

        except zipfile.BadZipFile as e:
            logger.error(f'Invalid ZIP file for app {appid}: {e}')
            raise Exception(f"Invalid ZIP file: {str(e)}")
        except Exception as e:
            logger.error(f'Failed to extract ZIP for app {appid}: {e}')
            raise

    def _extract_lua_and_manifest_from_zip(self, appid: int, zip_path: str, endpoint: str) -> None:
        try:
            lua_target_dir = get_stplug_in_path()
            manifest_target_dir = get_depotcache_path()
            installed_files = []

            self._set_download_state(appid, {'status': 'extracting'})
            logger.log(f"Extracting ZIP file {zip_path} for app {appid}")

            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                file_list = zip_file.namelist()
                logger.log(f"ZIP contains {len(file_list)} files")

                self._set_download_state(appid, {'status': 'installing'})

                for file_name in file_list:
                    if file_name.endswith('/'):
                        continue

                    lower_name = file_name.lower()
                    base_name = os.path.basename(file_name)
                    if not base_name:
                        continue

                    if lower_name.endswith('.lua'):
                        dest_file = os.path.join(lua_target_dir, base_name)
                    elif lower_name.endswith('.manifest') or lower_name.endswith('.manifest.vdf'):
                        dest_file = os.path.join(manifest_target_dir, base_name)
                    else:
                        continue

                    try:
                        file_content = zip_file.read(file_name)
                        with open(dest_file, 'wb') as out:
                            out.write(file_content)
                        installed_files.append(dest_file)
                    except Exception as e:
                        logger.error(f"Failed to extract {file_name}: {e}")
                        continue

            if not installed_files:
                raise Exception("No files were successfully extracted from ZIP")

            logger.log(f"Successfully installed {len(installed_files)} files from {endpoint}")
            self._set_download_state(appid, {
                'installedFiles': installed_files,
                'installedPath': installed_files[0] if installed_files else None
            })

        except zipfile.BadZipFile as e:
            logger.error(f'Invalid ZIP file for app {appid}: {e}')
            raise Exception(f"Invalid ZIP file: {str(e)}")
        except Exception as e:
            logger.error(f'Failed to extract ZIP for app {appid}: {e}')
            raise

    def add_via_lua(self, appid: int, endpoints: Optional[List[str]] = None) -> Dict[str, Any]:
        try:
            appid = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        local_lua_path = os.path.abspath(os.path.join(self.backend_path, '..', '..', 'my-react-app', 'backend', 'LuaFiles', f'{appid}.lua'))
        lua_found = os.path.exists(local_lua_path)
        
        initial_status = 'Lua found, downloading...' if lua_found else 'Generating Lua, one moment...'

        self._set_download_state(appid, {
            'status': initial_status,
            'bytesRead': 0,
            'totalBytes': 0
        })

        available_endpoints = ['unified']
        if endpoints:
            available_endpoints = endpoints

        def safe_availability_check_wrapper(appid, endpoints_to_check):
            try:
                if lua_found:
                    self._install_local_lua(appid, local_lua_path)
                else:
                    self._check_availability_and_download(appid, endpoints_to_check)
            except Exception as e:
                logger.error(f"Unhandled error in availability check thread: {e}")
                self._set_download_state(appid, {
                    'status': 'failed',
                    'error': f'Availability check crashed: {str(e)}'
                })

        thread = threading.Thread(
            target=safe_availability_check_wrapper,
            args=(appid, available_endpoints),
            daemon=True
        )
        thread.start()

        return {'success': True}


    def _check_availability_and_download(self, appid: int, endpoints_to_check: List[str]) -> None:
        self._download_from_manilua_backend(appid, 'unified')

    def is_massive_online_app(self, appid: int) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        if not self.has_online_key():
            return {
                'success': False,
                'error': 'Online access key is required.',
                'requiresOnlineKey': True
            }

        client = get_global_client()
        if not client:
            return {'success': False, 'error': 'HTTP client is unavailable'}

        try:
            url = self._build_online_api_url(f'plugin/massive-online/{app_id_int}')
            headers = {'X-Online-Key': self._online_key} if self.has_online_key() else None
            result = client.get(url, extra_headers=headers)
            if not result.get('success'):
                classification = self._classify_online_key_error(
                    result.get('error') or 'Failed to check MassiveOnline list',
                    status_code=result.get('status_code'),
                )
                response: Dict[str, Any] = {'success': False, 'error': classification['message']}
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    response['requiresOnlineKey'] = True
                    if classification['banned']:
                        response['onlineKeyBanned'] = True
                return response

            data = result.get('data') or {}
            if isinstance(data, dict) and data.get('success') is False:
                classification = self._classify_online_key_error(
                    data.get('error') or 'Failed to check MassiveOnline list',
                    status_code=result.get('status_code'),
                    error_code=data.get('code'),
                )
                response: Dict[str, Any] = {'success': False, 'error': classification['message']}
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    response['requiresOnlineKey'] = True
                    if classification['banned']:
                        response['onlineKeyBanned'] = True
                return response

            massive_flag = False
            if isinstance(data, dict):
                massive_flag = bool(data.get('massiveOnline') or data.get('isMassiveOnline'))

            return {'success': True, 'massiveOnline': massive_flag}
        except Exception as error:
            logger.error(f'Failed to check MassiveOnline app {app_id_int}: {error}')
            return {'success': False, 'error': str(error)}

    def is_dgame_app(self, appid: int) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        api_key = self.get_api_key()
        if not api_key:
            return {'success': False, 'error': 'API key is required', 'requiresNewKey': True}

        if not self.has_online_key():
            return {'success': False, 'error': 'Online access key is required', 'requiresOnlineKey': True}

        client = get_global_client()
        if not client:
            return {'success': False, 'error': 'HTTP client unavailable'}

        try:
            url = self._build_plugin_api_url(f'd-games/{app_id_int}')
            headers = {'X-Online-Key': self._online_key} if self.has_online_key() else None
            result = client.get(url, auth_token=api_key, extra_headers=headers)
            if not result.get('success'):
                return {'success': False, 'error': result.get('error', 'Request failed')}

            data = result.get('data') or {}
            if isinstance(data, dict) and data.get('success') is False:
                return {
                    'success': False,
                    'error': data.get('error') or 'Failed to check D-Games status'
                }

            is_dgame = bool(data.get('isDGame'))
            return {'success': True, 'isDGame': is_dgame}
        except Exception as error:
            logger.error(f'Failed to check D-Games app {app_id_int}: {error}')
            return {'success': False, 'error': str(error)}

    def _install_dgame_token(self, appid: int, file_name: Optional[str], content: bytes) -> None:
        install_path = get_app_install_path(appid)
        if not install_path:
            raise Exception('Unable to locate the Steam installation path for this game.')

        tokens_dir = os.path.join(install_path, 'DGameTokens')
        os.makedirs(tokens_dir, exist_ok=True)

        temp_name = f'dgtoken_{appid}_{int(time.time())}'
        temp_path = os.path.join(self.backend_path, f'{temp_name}.tmp')
        with open(temp_path, 'wb') as temp_file:
            temp_file.write(content)

        target_name = file_name or f'token_{appid}'
        _, extension = os.path.splitext(target_name)
        lower_ext = extension.lower()

        try:
            if lower_ext == '.zip':
                with zipfile.ZipFile(temp_path) as archive:
                    archive.extractall(tokens_dir)
                os.remove(temp_path)
            else:
                sanitized = target_name.replace('/', '_').replace('\\', '_')
                destination = os.path.join(tokens_dir, sanitized)
                shutil.move(temp_path, destination)
        except zipfile.BadZipFile as error:
            os.remove(temp_path)
            raise Exception(f'Invalid token archive: {error}')

    def request_dgame_token(self, appid: int) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        api_key = self.get_api_key()
        if not api_key:
            return {'success': False, 'error': 'API key is required', 'requiresNewKey': True}

        if not self.has_online_key():
            return {'success': False, 'error': 'Online access key is required.', 'requiresOnlineKey': True}

        client = get_global_client()
        if not client:
            return {'success': False, 'error': 'HTTP client unavailable'}

        headers = {'X-Online-Key': self._online_key} if self.has_online_key() else None

        try:
            url = self._build_plugin_api_url(f'd-games/{app_id_int}/request')
            result = client.post(url, auth_token=api_key, extra_headers=headers)
            if not result.get('success'):
                return {'success': False, 'error': result.get('error', 'Request failed')}

            data = result.get('data') or {}
            if isinstance(data, dict) and data.get('success') is False:
                response: Dict[str, Any] = {'success': False, 'error': data.get('error') or 'Request failed'}
                if data.get('requiresOnlineKey'):
                    response['requiresOnlineKey'] = True
                return response

            download_url = None
            file_name = None
            if isinstance(data, dict):
                download_url = data.get('downloadUrl')
                file_name = data.get('fileName')

            if download_url:
                absolute_download = urljoin(f'{API_BASE_URL}/', download_url.lstrip('/'))
                download_result = client.get_binary(absolute_download, auth_token=api_key, extra_headers=headers)
                if not download_result.get('success'):
                    raise Exception(download_result.get('error') or 'Failed to download token file')
                binary_data = download_result.get('data')
                if not isinstance(binary_data, (bytes, bytearray)):
                    raise Exception('Token download did not return binary data')
                self._install_dgame_token(app_id_int, file_name, binary_data)
                return {
                    'success': True,
                    'downloaded': True,
                    'message': 'Token downloaded successfully.'
                }

            message = data.get('message') if isinstance(data, dict) else 'Token request submitted.'
            return {'success': True, 'message': message}
        except Exception as error:
            logger.error(f'Failed to request D-Games token for {app_id_int}: {error}')
            return {'success': False, 'error': str(error)}

    def _extract_online_archive(self, appid: int, rar_path: str, target_dir: str, unrar_path: Optional[str]) -> None:
        executable = None

        if unrar_path and os.path.exists(unrar_path):
            executable = unrar_path
        else:
            executable = 'unrar'

        try:
            os.makedirs(target_dir, exist_ok=True)
            command = [executable, 'x', f'-p{self._online_password}', '-y', rar_path, target_dir]
            logger.log(f"Extracting online archive for {appid} using {executable}")

            startupinfo = None
            creationflags = 0
            if os.name == 'nt':
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= getattr(subprocess, 'STARTF_USESHOWWINDOW', 0)
                creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
        except FileNotFoundError:
            if executable != 'unrar':
                raise
            raise Exception('unrar executable is not available on this system')
        except Exception as error:
            raise Exception(f'Failed to run unrar: {error}')

        if result.returncode != 0:
            stderr_text = ''
            try:
                stderr_text = result.stderr.strip() if isinstance(result.stderr, str) else ''
            except Exception:
                stderr_text = ''
            raise Exception(f'unrar returned {result.returncode}: {stderr_text or "Unknown error"}')

    def get_online_availability(self, appid: int) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        client = get_global_client()
        if not client:
            return {'success': False, 'error': 'HTTP client is unavailable'}

        try:
            url = self._build_online_api_url(f'plugin/online-files/{app_id_int}/availability')
            headers = {'X-Online-Key': self._online_key} if self.has_online_key() else None
            result = client.get(url, extra_headers=headers)
            if not result.get('success'):
                classification = self._classify_online_key_error(
                    result.get('error'),
                    status_code=result.get('status_code'),
                )
                response: Dict[str, Any] = {
                    'success': False,
                    'error': classification['message'],
                }
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    response['requiresOnlineKey'] = True
                    if classification['banned']:
                        response['onlineKeyBanned'] = True
                return response

            data = result.get('data') or {}
            if isinstance(data, dict):
                if data.get('success') is False:
                    error_code = data.get('code') if isinstance(data.get('code'), str) else None
                    classification = self._classify_online_key_error(
                        data.get('error'),
                        error_code=error_code,
                    )
                    if classification['requires_reset']:
                        self._handle_invalid_online_key(classification['banned'], classification['message'])
                        data = {
                            **data,
                            'error': classification['message'],
                            'requiresOnlineKey': True,
                        }
                        if classification['banned']:
                            data['onlineKeyBanned'] = True
                    return data

                return data

            return {'success': False, 'error': 'Unexpected availability response'}
        except Exception as error:
            logger.error(f'Availability check failed for {app_id_int}: {error}')
            return {'success': False, 'error': str(error)}

    def _download_online_files(self, appid: int) -> None:
        rar_path: Optional[str] = None
        temp_unrar_path: Optional[str] = None

        try:
            online_key = self.get_online_key()
            if not online_key:
                raise Exception('Online access key is not configured')

            install_path = get_app_install_path(appid)
            if not install_path:
                raise Exception('Steam install directory for this game was not found')

            os.makedirs(install_path, exist_ok=True)

            client = get_global_client()
            if not client:
                raise Exception('HTTP client is unavailable')

            self._set_download_state(appid, {
                'status': 'checking_availability',
                'bytesRead': 0,
                'totalBytes': 0,
                'endpoint': 'online',
                'mode': 'online',
                'currentApi': 'online-files'
            })

            metadata_url = self._build_online_api_url(f'plugin/online-files/{appid}')
            metadata_result = client.get(metadata_url, extra_headers={'X-Online-Key': online_key})

            if not metadata_result.get('success'):
                classification = self._classify_online_key_error(
                    metadata_result.get('error') or 'Online archive not found',
                    status_code=metadata_result.get('status_code'),
                )
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    raise OnlineKeyAuthorizationError(classification['message'], banned=classification['banned'])
                raise Exception(classification['message'])

            metadata = metadata_result.get('data') or {}
            if metadata.get('success') is False:
                error_code = metadata.get('code') if isinstance(metadata.get('code'), str) else None
                classification = self._classify_online_key_error(
                    metadata.get('error') or 'Archive unavailable',
                    error_code=error_code,
                )
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    raise OnlineKeyAuthorizationError(classification['message'], banned=classification['banned'])
                raise Exception(classification['message'])

            rar_url = metadata.get('rarUrl')
            if not rar_url:
                raise Exception('RAR archive URL is missing')

            absolute_rar_url = urljoin(f'{ONLINE_API_BASE_URL}/', str(rar_url))
            total_bytes = 0
            try:
                total_bytes = int(metadata.get('rarSize') or 0)
            except Exception:
                total_bytes = 0

            rar_path = os.path.join(install_path, f'{appid}.rar')
            bytes_read = 0
            last_update = 0.0

            with client.stream_get(absolute_rar_url, extra_headers={'X-Online-Key': online_key}) as response:
                status_code = getattr(response, 'status_code', None)
                if status_code in (401, 403):
                    classification = self._classify_online_key_error(
                        'Online access key was rejected by the server' if status_code == 401 else 'Online access key has been banned or revoked by the server.',
                        status_code=status_code,
                    )
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    raise OnlineKeyAuthorizationError(classification['message'], banned=classification['banned'])
                
                if status_code == 429:
                     self._set_download_state(appid, {
                        'status': 'failed',
                        'error': 'You ran out of points, check the user panel for more details; please wait for the weekly reset or consider getting premium from https://mugi.store',
                        'endpoint': 'online',
                        'mode': 'online'
                    })
                     return

                if not getattr(response, 'is_success', False):
                    reason = getattr(response, 'reason_phrase', '')
                    raise Exception(f'HTTP {status_code or "?"} while downloading archive {reason}')

                try:
                    header_total = response.headers.get('Content-Length')
                    if header_total and total_bytes == 0:
                        total_bytes = int(header_total)
                except Exception:
                    total_bytes = total_bytes or 0

                self._set_download_state(appid, {
                    'status': 'downloading',
                    'bytesRead': 0,
                    'totalBytes': total_bytes,
                    'endpoint': 'online',
                    'mode': 'online'
                })

                with open(rar_path, 'wb', buffering=HTTP_CHUNK_SIZE) as rar_file:
                    for chunk in response.iter_bytes(chunk_size=HTTP_CHUNK_SIZE):
                        if not chunk:
                            continue
                        rar_file.write(chunk)
                        bytes_read += len(chunk)

                        try:
                            import time as _time
                            now_ts = _time.time()
                        except Exception:
                            now_ts = 0.0

                        if last_update == 0.0 or (now_ts - last_update) >= DOWNLOAD_PROGRESS_UPDATE_INTERVAL:
                            self._set_download_state(appid, {
                                'status': 'downloading',
                                'bytesRead': bytes_read,
                                'totalBytes': total_bytes,
                                'endpoint': 'online',
                                'mode': 'online'
                            })
                            last_update = now_ts

            if bytes_read <= 0:
                raise Exception('Downloaded archive was empty')

            self._set_download_state(appid, {
                'status': 'processing',
                'bytesRead': bytes_read,
                'totalBytes': total_bytes,
                'endpoint': 'online',
                'mode': 'online'
            })

            if metadata.get('unrarAvailable') and metadata.get('unrarUrl'):
                unrar_url = urljoin(f'{ONLINE_API_BASE_URL}/', str(metadata.get('unrarUrl')))
                unrar_result = client.get_binary(unrar_url, extra_headers={'X-Online-Key': online_key})
                if not unrar_result.get('success'):
                    classification = self._classify_online_key_error(
                        unrar_result.get('error') or 'Failed to download unrar executable',
                        status_code=unrar_result.get('status_code'),
                    )
                    if classification['requires_reset']:
                        self._handle_invalid_online_key(classification['banned'], classification['message'])
                        raise OnlineKeyAuthorizationError(classification['message'], banned=classification['banned'])
                    raise Exception(classification['message'])
                temp_unrar_path = os.path.join(install_path, 'unrar.exe')
                try:
                    with open(temp_unrar_path, 'wb') as unrar_file:
                        unrar_file.write(unrar_result.get('data') or b'')
                except Exception as error:
                    raise Exception(f'Could not write unrar executable: {error}')

            self._set_download_state(appid, {
                'status': 'extracting',
                'endpoint': 'online',
                'mode': 'online'
            })

            self._extract_online_archive(appid, rar_path, install_path, temp_unrar_path)

            self._set_download_state(appid, {
                'status': 'installing',
                'installedPath': install_path,
                'endpoint': 'online',
                'mode': 'online'
            })

            self._set_download_state(appid, {
                'status': 'done',
                'success': True,
                'api': 'online-files',
                'endpoint': 'online',
                'mode': 'online'
            })

        except OnlineKeyAuthorizationError as error:
            error_message = str(error)
            logger.error(f"Online download failed for {appid}: {error_message}")
            state_update = {
                'status': 'failed',
                'error': error_message,
                'endpoint': 'online',
                'mode': 'online',
                'requiresOnlineKey': True,
            }
            if getattr(error, 'banned', False):
                state_update['onlineKeyBanned'] = True
            self._set_download_state(appid, state_update)
        except Exception as error:
            error_message = str(error)
            logger.error(f"Online download failed for {appid}: {error_message}")
            classification = self._classify_online_key_error(error_message)
            state_update = {
                'status': 'failed',
                'error': classification['message'] if classification['requires_reset'] else error_message,
                'endpoint': 'online',
                'mode': 'online'
            }
            if classification['requires_reset']:
                self._handle_invalid_online_key(classification['banned'], classification['message'])
                state_update['requiresOnlineKey'] = True
                if classification['banned']:
                    state_update['onlineKeyBanned'] = True
            self._set_download_state(appid, state_update)
        finally:
            for path_to_remove in [rar_path, temp_unrar_path]:
                if path_to_remove and os.path.exists(path_to_remove):
                    try:
                        os.remove(path_to_remove)
                    except Exception:
                        continue

    def add_via_online(self, appid: int) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        if not self.has_online_key():
            return {'success': False, 'error': 'Online access key is required before downloading.', 'requiresOnlineKey': True}

        availability = self.get_online_availability(app_id_int)
        if isinstance(availability, dict):
            availability_success = availability.get('success')
            availability_available = availability.get('available')
            if availability_success is False or availability_available is False:
                response: Dict[str, Any] = {
                    'success': False,
                    'error': availability.get('error') or availability.get('message') or 'Online files are not available yet.',
                    'canRequest': bool(availability.get('canRequest')),
                    'requestStatus': availability.get('requestStatus'),
                    'requestId': availability.get('requestId'),
                    'gameName': availability.get('gameName'),
                    'supportCount': availability.get('supportCount')
                }
                if 'available' in availability:
                    response['available'] = availability_available
                if availability.get('message'):
                    response['message'] = availability.get('message')
                if availability.get('requiresOnlineKey'):
                    response['requiresOnlineKey'] = True
                if availability.get('onlineKeyBanned'):
                    response['onlineKeyBanned'] = True
                return response
            if availability_success is False and availability_available is None:
                response: Dict[str, Any] = {'success': False, 'error': availability.get('error') or 'Online files are unavailable'}
                if availability.get('requiresOnlineKey'):
                    response['requiresOnlineKey'] = True
                if availability.get('onlineKeyBanned'):
                    response['onlineKeyBanned'] = True
                return response

        self._set_download_state(app_id_int, {
            'status': 'queued',
            'bytesRead': 0,
            'totalBytes': 0,
            'endpoint': 'online',
            'mode': 'online'
        })

        thread = threading.Thread(
            target=self._download_online_files,
            args=(app_id_int,),
            daemon=True
        )
        thread.start()

        return {'success': True}

    def request_online_file(self, appid: int, game_name: Optional[str] = None) -> Dict[str, Any]:
        try:
            app_id_int = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        client = get_global_client()
        if not client:
            return {'success': False, 'error': 'HTTP client is unavailable'}

        payload: Dict[str, Any] = {}
        if isinstance(game_name, str) and game_name.strip():
            payload['gameName'] = game_name.strip()

        try:
            url = self._build_online_api_url(f'plugin/online-files/{app_id_int}/request')
            headers = {'X-Online-Key': self._online_key} if self.has_online_key() else None
            result = client.post(url, data=payload, extra_headers=headers)
            if not result.get('success'):
                classification = self._classify_online_key_error(
                    result.get('error') or 'Failed to submit request',
                    status_code=result.get('status_code'),
                )
                response: Dict[str, Any] = {
                    'success': False,
                    'error': classification['message'],
                }
                if classification['requires_reset']:
                    self._handle_invalid_online_key(classification['banned'], classification['message'])
                    response['requiresOnlineKey'] = True
                    if classification['banned']:
                        response['onlineKeyBanned'] = True
                return response
            data = result.get('data') or {}
            if isinstance(data, dict):
                if data.get('success') is False:
                    error_code = data.get('code') if isinstance(data.get('code'), str) else None
                    classification = self._classify_online_key_error(
                        data.get('error'),
                        error_code=error_code,
                    )
                    if classification['requires_reset']:
                        self._handle_invalid_online_key(classification['banned'], classification['message'])
                        data = {
                            **data,
                            'error': classification['message'],
                            'requiresOnlineKey': True,
                        }
                        if classification['banned']:
                            data['onlineKeyBanned'] = True
                    return data
                return data
            return {'success': False, 'error': 'Unexpected request response'}
        except Exception as error:
            logger.error(f'Failed to request online files for {app_id_int}: {error}')
            return {'success': False, 'error': str(error)}

    def remove_via_lua(self, appid: int) -> Dict[str, Any]:
        try:
            appid = int(appid)
        except (ValueError, TypeError):
            return {'success': False, 'error': 'Invalid appid'}

        try:
            stplug_path = get_stplug_in_path()
            removed_files = []

            lua_file = os.path.join(stplug_path, f'{appid}.lua')
            if os.path.exists(lua_file):
                os.remove(lua_file)
                removed_files.append(f'{appid}.lua')
                logger.log(f"Removed {lua_file}")

            disabled_file = os.path.join(stplug_path, f'{appid}.lua.disabled')
            if os.path.exists(disabled_file):
                os.remove(disabled_file)
                removed_files.append(f'{appid}.lua.disabled')
                logger.log(f"Removed {disabled_file}")

            for filename in os.listdir(stplug_path):
                if filename.startswith(f'{appid}_') and filename.endswith('.manifest'):
                    manifest_file = os.path.join(stplug_path, filename)
                    os.remove(manifest_file)
                    removed_files.append(filename)
                    logger.log(f"Removed {manifest_file}")

            if removed_files:
                logger.log(f"Successfully removed {len(removed_files)} files for app {appid}: {removed_files}")
                return {'success': True, 'message': f'Removed {len(removed_files)} files', 'removed_files': removed_files}
            else:
                return {'success': False, 'error': f'No files found for app {appid}'}

        except Exception as e:
            logger.error(f"Error removing files for app {appid}: {e}")
            return {'success': False, 'error': str(e)}
