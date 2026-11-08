from ipaddress import ip_address
from math import isfinite
from typing import Protocol

import requests
from urllib3.util import Timeout

from .errors import ProxySourceError
from .models import PROTOCOLS, Deadline, Filters, Proxy

API_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get"
PAGE_SIZE = 2000


class ProxySource(Protocol):
    def fetch(self, filters: Filters, deadline: Deadline) -> tuple[Proxy, ...]: ...


class ProxyScrapeSource:
    def fetch(self, filters: Filters, deadline: Deadline) -> tuple[Proxy, ...]:
        params = {
            "request": "display_proxies",
            "proxy_format": "protocolipport",
            "format": "json",
            "limit": PAGE_SIZE,
            "skip": 0,
        }
        if filters.country is not None:
            params["country"] = filters.country
        # The upstream "https" query includes HTTPS-capable SOCKS and HTTP
        # entries. Match the actual proxy protocol from each record below.
        if filters.protocol not in (None, "https"):
            params["protocol"] = filters.protocol

        candidates: dict[str, Proxy] = {}
        with requests.Session() as session:
            session.trust_env = False
            for _ in range(50):
                remaining = deadline.remaining()
                if not remaining:
                    raise ProxySourceError("ProxyScrape list refresh timed out")
                try:
                    response = session.get(API_URL, params=params, timeout=Timeout(total=remaining))
                    response.raise_for_status()
                    page = response.json()
                except (requests.RequestException, ValueError) as exc:
                    raise ProxySourceError("Could not fetch the ProxyScrape list") from exc
                if not deadline.remaining():
                    raise ProxySourceError("ProxyScrape list refresh timed out")
                if (
                    not isinstance(page, dict)
                    or not isinstance(page.get("proxies"), list)
                    or not isinstance(page.get("nextpage"), bool)
                    or page.get("skip") != params["skip"]
                ):
                    raise ProxySourceError("Invalid ProxyScrape list response")

                for record in page["proxies"]:
                    proxy = _parse_proxy(record, filters)
                    if proxy is not None:
                        candidates[proxy.url] = proxy

                if not page["nextpage"]:
                    return tuple(candidates.values())
                step = page.get("limit")
                if type(step) is not int or not 0 < step <= PAGE_SIZE or not page["proxies"]:
                    raise ProxySourceError("Invalid ProxyScrape pagination")
                params["skip"] += step

        raise ProxySourceError("ProxyScrape page limit exceeded")


def _metric(value: object, upper: float = float("inf")) -> float | None:
    if type(value) in (int, float) and 0 <= value <= min(upper, 1e12) and isfinite(value):
        return float(value)
    return None


def _parse_proxy(record: object, filters: Filters) -> Proxy | None:
    if not isinstance(record, dict) or record.get("alive") is not True:
        return None
    protocol = record.get("protocol")
    port = record.get("port")
    geo = record.get("ip_data")
    if not isinstance(protocol, str) or protocol not in PROTOCOLS:
        return None
    if filters.protocol is not None and protocol != filters.protocol:
        return None
    if type(port) is not int or not 1 <= port <= 65535:
        return None
    country = geo.get("countryCode") if isinstance(geo, dict) else None
    country = country.lower() if isinstance(country, str) else ""
    if filters.country is not None and country != filters.country:
        return None
    ip = record.get("ip")
    if not isinstance(ip, str) or "%" in ip:
        return None
    try:
        address = ip_address(ip)
    except ValueError:
        return None
    if not address.is_global or address.is_multicast:
        return None
    host = f"[{address}]" if address.version == 6 else str(address)
    latency = _metric(record.get("average_timeout"))
    ssl = record.get("ssl")
    return Proxy(
        url=f"{protocol}://{host}:{port}",
        protocol=protocol,
        country=country,
        uptime=_metric(record.get("uptime"), 100),
        latency_ms=latency if latency is not None else _metric(record.get("timeout")),
        supports_https=ssl if isinstance(ssl, bool) else None,
    )
