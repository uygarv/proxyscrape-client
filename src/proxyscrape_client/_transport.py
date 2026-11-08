from typing import Any

import requests
from urllib3.util import Timeout

from .models import Proxy


class ProxyTransportError(Exception):
    """A connection or proxy failure eligible for a new attempt."""


def send(
    method: str,
    url: str,
    proxy: Proxy,
    timeout: float,
    options: dict[str, Any],
) -> requests.Response:
    scheme = {"socks4": "socks4a", "socks5": "socks5h"}.get(proxy.protocol, proxy.protocol)
    proxy_url = scheme + proxy.url[len(proxy.protocol):]
    with requests.Session() as session:
        session.trust_env = False
        try:
            response = session.request(
                method,
                url,
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=Timeout(total=timeout),
                **options,
            )
        except requests.exceptions.SSLError:
            raise
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as exc:
            raise ProxyTransportError("Proxy connection failed") from exc

    if response.status_code == 407:
        response.close()
        raise ProxyTransportError("Proxy requires authentication")
    return response
