import os
import hashlib
import time
import random
import platform
import subprocess
from typing import Dict, Optional, Any

import Millennium
import PluginUtils

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False

try:
    import winreg  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - winreg only exists on Windows
    winreg = None

logger = PluginUtils.Logger()

_EXCLUDED_DIRS = {'__pycache__', '.git', '.idea', '.vscode'}
_EXCLUDED_SUFFIXES = {'.pyc', '.pyo', '.log', '.tmp', '.bak', '.swp'}
_EXCLUDED_FILENAMES = {'.DS_Store', 'online_key.txt'}


def _iter_backend_files(base_dir: str):
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [
            directory
            for directory in sorted(dirs)
            if directory not in _EXCLUDED_DIRS and not directory.lower().endswith('.egg-info')
        ]
        for filename in sorted(files):
            if filename in _EXCLUDED_FILENAMES:
                continue
            lowered = filename.lower()
            if any(lowered.endswith(suffix) for suffix in _EXCLUDED_SUFFIXES):
                continue
            absolute_path = os.path.join(root, filename)
            if not os.path.isfile(absolute_path):
                continue
            relative_path = os.path.relpath(absolute_path, base_dir).replace('\\', '/')
            yield relative_path, absolute_path


class SteamVerification:
    def __init__(self):
        self.steam_pid = None
        self.steam_process = None
        self.millennium_version = None
        self.plugin_checksum: Optional[str] = None
        self.device_fingerprint: Optional[str] = None
        self.processor_id: Optional[str] = None
        self._discover_steam_process()
        self._calculate_plugin_checksum()
        self._calculate_device_fingerprint()

    def _discover_steam_process(self):
        try:
            if not PSUTIL_AVAILABLE:
                self.steam_pid = random.randint(1000, 65535)
            elif psutil is not None:
                for proc in psutil.process_iter(['pid', 'name', 'exe']):
                    try:
                        proc_info = proc.info
                        if proc_info['name'] and 'steam' in proc_info['name'].lower():
                            if proc_info['exe'] and 'steam.exe' in proc_info['exe'].lower():
                                self.steam_pid = proc_info['pid']
                                self.steam_process = proc
                                break
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue
                if not self.steam_pid:
                    self.steam_pid = random.randint(1000, 65535)
            try:
                self.millennium_version = Millennium.version()
            except Exception:
                self.millennium_version = "1.0.0"
        except Exception as error:
            logger.error(f"manilua (steam_verification): Error discovering Steam process: {error}")
            self.steam_pid = random.randint(1000, 65535)
            self.millennium_version = "1.0.0"

    def _calculate_plugin_checksum(self):
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        hasher = hashlib.sha256()
        files_hashed = 0
        try:
            for relative_path, absolute_path in _iter_backend_files(backend_dir):
                try:
                    with open(absolute_path, 'rb') as handle:
                        content = handle.read()
                except Exception as file_error:
                    logger.warn(
                        f"manilua (steam_verification): Skipped {absolute_path} during checksum calculation: {file_error}"
                    )
                    continue
                hasher.update(relative_path.encode('utf-8'))
                hasher.update(content)
                files_hashed += 1
            if files_hashed == 0:
                raise RuntimeError('No backend files were included in checksum calculation')
            self.plugin_checksum = hasher.hexdigest()
        except Exception as error:
            logger.error(f"manilua (steam_verification): Error calculating plugin checksum: {error}")
            fallback_data = f"{time.time()}-{os.environ.get('USERNAME', 'unknown')}-{self.steam_pid or 0}"
            self.plugin_checksum = hashlib.sha256(fallback_data.encode()).hexdigest()

    def _get_processor_id(self) -> Optional[str]:
        if getattr(self, 'processor_id', None):
            return self.processor_id

        processor_id: Optional[str] = None
        try:
            system = platform.system().strip().lower()
        except Exception:
            system = ''

        if system == 'windows':
            commands = [
                ['wmic', 'cpu', 'get', 'ProcessorId'],
                [
                    'powershell',
                    '-NoProfile',
                    '-Command',
                    'Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty ProcessorId',
                ],
            ]
            startupinfo = None
            creationflags = 0
            if os.name == 'nt':  # pragma: no cover - Windows-specific configuration
                try:
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= getattr(subprocess, 'STARTF_USESHOWWINDOW', 0)
                except AttributeError:
                    startupinfo = None
                creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)

            for command in commands:
                try:
                    completed = subprocess.run(
                        command,
                        capture_output=True,
                        text=True,
                        check=True,
                        timeout=5,
                        startupinfo=startupinfo,
                        creationflags=creationflags,
                    )
                    output = completed.stdout.strip()
                    if not output:
                        output = completed.stderr.strip()
                    if output:
                        lines = [
                            line.strip()
                            for line in output.splitlines()
                            if line.strip() and 'processorid' not in line.lower()
                        ]
                        if lines:
                            processor_id = lines[0]
                            break
                except (subprocess.SubprocessError, OSError, ValueError):
                    continue

            if processor_id is None and winreg is not None:
                try:
                    with winreg.OpenKey(
                        winreg.HKEY_LOCAL_MACHINE,
                        r'HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0',
                    ) as key:
                        value, _ = winreg.QueryValueEx(key, 'ProcessorId')
                        if isinstance(value, str) and value.strip():
                            processor_id = value.strip()
                except OSError:
                    processor_id = None

        if processor_id is None and system in {'linux', 'freebsd', 'darwin'}:
            candidates = ['/proc/cpuinfo'] if system == 'linux' else []
            for path in candidates:
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as handle:
                        for line in handle:
                            lowered = line.lower()
                            if 'serial' in lowered or 'processorid' in lowered:
                                _, _, value = line.partition(':')
                                candidate = value.strip()
                                if candidate:
                                    processor_id = candidate
                                    break
                        if processor_id:
                            break
                except OSError:
                    continue

        if processor_id:
            normalized = ''.join(ch for ch in processor_id if ch.isalnum())
            return normalized.lower() or processor_id.lower()

        return None

    def _calculate_device_fingerprint(self):
        try:
            processor_id = self._get_processor_id()
            if processor_id:
                identifier_source = processor_id
                self.processor_id = processor_id
            else:
                identifier_source = 'processor-unknown'
            self.device_fingerprint = hashlib.sha256(identifier_source.encode()).hexdigest()
        except Exception as error:
            logger.error(f"manilua (steam_verification): Error calculating device fingerprint: {error}")
            fallback_data = f"device-{time.time()}-{random.randint(0, 1_000_000)}"
            self.device_fingerprint = hashlib.sha256(fallback_data.encode()).hexdigest()

    def _get_process_hash(self) -> str:
        try:
            if self.steam_process:
                memory_info = self.steam_process.memory_info()
                cpu_percent = self.steam_process.cpu_percent()
                create_time = self.steam_process.create_time()
                process_data = f"{memory_info.rss}-{memory_info.vms}-{cpu_percent}-{create_time}"
                return hashlib.sha256(process_data.encode()).hexdigest()[:32]
        except Exception as error:
            logger.warn(f"manilua (steam_verification): Could not get process metrics for session token: {error}")
        fallback_data = f"{time.time()}-{self.steam_pid}" if self.steam_pid else f"{time.time()}"
        return hashlib.sha256(fallback_data.encode()).hexdigest()[:32]

    def _get_memory_proof(self) -> str:
        try:
            if self.steam_process:
                threads = len(self.steam_process.threads())
                memory_maps = len(self.steam_process.memory_maps()) if hasattr(self.steam_process, 'memory_maps') else 0
                memory_data = f"{threads}-{memory_maps}-{self.steam_pid}"
                return hashlib.sha256(memory_data.encode()).hexdigest()[:32]
        except Exception as error:
            logger.warn(f"manilua (steam_verification): Could not get memory metrics for memory token: {error}")
        fallback_data = f"memory-{self.steam_pid}-{time.time()}"
        return hashlib.sha256(fallback_data.encode()).hexdigest()[:32]

    def get_verification_headers(self) -> Dict[str, str]:
        current_time = str(int(time.time() * 1000))
        headers = {
            'X-Steam-PID': str(self.steam_pid) if self.steam_pid else '0',
            'X-Millennium-Version': self.millennium_version or '1.0.0',
            'X-Plugin-Checksum': self.plugin_checksum or '',
            'X-Device-Fingerprint': self.device_fingerprint or '',
            'X-Process-Hash': self._get_process_hash(),
            'X-Memory-Proof': self._get_memory_proof(),
            'X-Plugin-Timestamp': current_time,
            'User-Agent': f'manilua-plugin/{self.millennium_version} (Millennium)',
        }
        return headers

    def refresh_verification(self):
        try:
            if self.steam_process and not self.steam_process.is_running():
                logger.log("manilua (steam_verification): Steam process changed, refreshing...")
                self._discover_steam_process()
            if random.random() < 0.1 or not self.plugin_checksum:
                self._calculate_plugin_checksum()
            if random.random() < 0.1 or not self.device_fingerprint:
                self._calculate_device_fingerprint()
        except Exception as error:
            logger.error(f"manilua (steam_verification): Error refreshing verification: {error}")

    def get_steam_info(self) -> Dict[str, Any]:
        info = {
            'steam_pid': self.steam_pid,
            'millennium_version': self.millennium_version,
            'has_process': self.steam_process is not None,
            'checksum_length': len(self.plugin_checksum) if self.plugin_checksum else 0,
            'device_fingerprint_length': len(self.device_fingerprint) if self.device_fingerprint else 0,
            'processor_id_length': len(self.processor_id) if self.processor_id else 0,
        }
        if self.steam_process:
            try:
                info.update({
                    'process_name': self.steam_process.name(),
                    'process_running': self.steam_process.is_running(),
                    'memory_rss': self.steam_process.memory_info().rss
                })
            except Exception as error:
                logger.warn(f"manilua (steam_verification): Could not get process debug info: {error}")
        return info


_verification_instance: Optional[SteamVerification] = None


def get_steam_verification() -> SteamVerification:
    global _verification_instance
    if _verification_instance is None:
        _verification_instance = SteamVerification()
    return _verification_instance


def refresh_steam_verification():
    global _verification_instance
    if _verification_instance:
        _verification_instance.refresh_verification()
