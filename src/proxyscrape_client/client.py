from time import monotonic
from typing import Any
from urllib.parse import urlsplit

import requests

from . import _transport
from ._pool import Origin, ProxyPool
from ._provider import ProxyScrapeSource
from .errors import NoProxyAvailable, ProxyRequestError
from .models import Deadline, Filters, Proxy, positive_seconds

_SAFE_METHODS = frozenset({"GET", "HEAD"})


def _origin(url: str) -> Origin:
    parsed = urlsplit(url)
    host = parsed.hostname
    if parsed.scheme not in {"http", "https"} or not host:
        raise ValueError("url must be an absolute HTTP or HTTPS URL")
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port
    return parsed.scheme, host, default_port if port is None else port


class Client:
    """Synchronous Requests client backed by a rotating ProxyScrape pool."""

    def __init__(
        self,
        *,
        country: str | None = None,
        protocol: str | None = None,
        refresh_interval: float = 60,
        max_stale: float = 300,
        source_timeout: float = 8,
        cooldown: float = 30,
        attempt_timeout: float = 4,
    ) -> None:
        self._defaults = Filters(country, protocol)
        self._source_timeout = positive_seconds(source_timeout, "source_timeout")
        self._attempt_timeout = positive_seconds(attempt_timeout, "attempt_timeout")
        refresh_interval = positive_seconds(refresh_interval, "refresh_interval")
        max_stale = positive_seconds(max_stale, "max_stale")
        if max_stale < refresh_interval:
            raise ValueError("max_stale must be at least refresh_interval")
        self._pool = ProxyPool(
            ProxyScrapeSource(),
            refresh_interval,
            max_stale,
            self._source_timeout,
            positive_seconds(cooldown, "cooldown"),
        )

    def _filters(self, country: str | None, protocol: str | None) -> Filters:
        return Filters(
            self._defaults.country if country is None else country,
            self._defaults.protocol if protocol is None else protocol,
        )

    def list_proxies(
        self, *, country: str | None = None, protocol: str | None = None
    ) -> list[Proxy]:
        """Return matching candidates ranked by source metadata."""
        filters = self._filters(country, protocol)
        return self._pool.ranked(self._pool.snapshot(filters, Deadline.after(self._source_timeout)))

    def request(
        self,
        method: str,
        url: str,
        *,
        country: str | None = None,
        protocol: str | None = None,
        timeout: float = 15,
        max_attempts: int = 3,
        retry_unsafe: bool = False,
        **options: Any,
    ) -> requests.Response:
        """Send a buffered request through one or more eligible proxies."""
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if "proxies" in options:
            raise ValueError("proxy selection is managed by the client")
        if options.get("stream"):
            raise ValueError("streaming responses are not supported")
        verify = options.get("verify")
        if verify is not None and not verify:
            raise ValueError("TLS verification must stay enabled")

        method = method.upper()
        origin = _origin(url)
        filters = self._filters(country, protocol)
        deadline = Deadline.after(positive_seconds(timeout, "timeout"))
        # Retry buffered bodies only. File handles and iterators may have been
        # consumed by the previous attempt.
        body = options.get("data")
        replayable = not options.get("files") and (
            body is None or isinstance(body, (bytes, str))
        )
        if not replayable or (method not in _SAFE_METHODS and not retry_unsafe):
            max_attempts = 1

        proxies = self._pool.ranked(self._pool.snapshot(filters, deadline), origin)
        if not proxies:
            raise NoProxyAvailable(
                f"No eligible proxy for country={filters.country!r}, protocol={filters.protocol!r}"
            )
        attempts = 0
        last_error: Exception | None = None
        for proxy in proxies[:max_attempts]:
            remaining = deadline.remaining()
            if not remaining:
                break
            attempts += 1
            started = monotonic()
            try:
                response = _transport.send(
                    method, url, proxy, min(remaining, self._attempt_timeout), options
                )
            except _transport.ProxyTransportError as exc:
                self._pool.record(origin, proxy, None)
                last_error = exc
                continue
            self._pool.record(origin, proxy, monotonic() - started)
            if deadline.remaining():
                return response
            response.close()
            break
        message = (
            "Proxy request exceeded its time budget"
            if not deadline.remaining()
            else "Proxy request failed on all attempted proxies"
        )
        raise ProxyRequestError(message, attempts=attempts) from last_error

    def get(self, url: str, **options: Any) -> requests.Response:
        return self.request("GET", url, **options)

    def head(self, url: str, **options: Any) -> requests.Response:
        options.setdefault("allow_redirects", False)
        return self.request("HEAD", url, **options)

    def post(self, url: str, **options: Any) -> requests.Response:
        return self.request("POST", url, **options)

    def close(self) -> None:
        self._pool.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
