import os

VERSION = '1.6.1'

API_BASE_URL = 'https://mugi.store/api/plugin'
ONLINE_API_BASE_URL = 'https://mugi.store'
API_KEY_PREFIX = 'manilua_'
API_KEY_ALLOWED_PREFIXES = tuple(
    prefix
    for prefix in {
        API_KEY_PREFIX,
        'mugi_',
        'premium_',
        'nitro_',
        *[
            item.strip()
            for item in os.environ.get('MANILUA_API_KEY_ALLOWED_PREFIXES', '').split(',')
            if item.strip()
        ],
    }
    if prefix
)
DEFAULT_API_KEY = 'manilua_3sBFELE5Z4pG1yHFzTZOqbQSE6cFn9Hg'

D_TYPE = 1

HTTP_TIMEOUT_DEFAULT = 30
HTTP_MAX_RETRIES = 5
HTTP_BASE_RETRY_DELAY = 2.0
HTTP_CHUNK_SIZE = 512 * 1024

DOWNLOAD_PROGRESS_UPDATE_INTERVAL = 0.5

USER_AGENT = f'manilua-plugin/{VERSION} (Millennium)'

PLUGIN_UPDATE_URL = 'https://mugi.store/download/plugin.zip'
PLUGIN_VERSION_RESET = '1.6.1'
