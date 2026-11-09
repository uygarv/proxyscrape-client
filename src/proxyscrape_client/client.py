from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, SimpleQueue
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
        preflight_concurrency: int = 3,
        preflight_max_attempts: int = 9,
    ) -> None:
        if type(preflight_concurrency) is not int or preflight_concurrency < 1:
            raise ValueError("preflight_concurrency must be a positive integer")
        if type(preflight_max_attempts) is not int or preflight_max_attempts < 1:
            raise ValueError("preflight_max_attempts must be a positive integer")
        self._defaults = Filters(country, protocol)
        self._source_timeout = positive_seconds(source_timeout, "source_timeout")
        self._attempt_timeout = positive_seconds(attempt_timeout, "attempt_timeout")
        self._preflight_concurrency = preflight_concurrency
        self._preflight_max_attempts = preflight_max_attempts
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

    def _probe(self, url: str, proxy: Proxy, deadline: Deadline, options: dict[str, Any]) -> None:
        remaining = deadline.remaining()
        if not remaining:
            raise _transport.ProxyTransportError("Proxy preflight exceeded its time budget")
        with _transport.send("HEAD", url, proxy, min(remaining, self._attempt_timeout), options) as response:
            status = response.status_code
        if not (200 <= status < 400 or status in (401, 404, 405)):
            raise _transport.ProxyTransportError(f"Proxy preflight returned HTTP {status}")

    def _select_proxy(
        self, url: str, proxies: list[Proxy], origin: Origin, deadline: Deadline, options: dict[str, Any]
    ) -> tuple[Proxy, int]:
        parsed = urlsplit(url)
        probe_url = parsed._replace(query="", fragment="").geturl()
        request_headers = requests.structures.CaseInsensitiveDict(options.get("headers") or {})
        probe_options = {
            "allow_redirects": False,
            "verify": options.get("verify") or True,
            "headers": {name: request_headers[name] for name in ("Accept", "User-Agent") if name in request_headers},
        }
        attempts = 0
        last_error: Exception | None = None
        completed: SimpleQueue[Future[None]] = SimpleQueue()
        batch_size = min(self._preflight_concurrency, len(proxies))
        executor = ThreadPoolExecutor(max_workers=batch_size)
        try:
            for start in range(0, len(proxies), batch_size):
                if not deadline.remaining():
                    break
                batch = proxies[start:start + batch_size]
                probes = {}
                for proxy in batch:
                    future = executor.submit(self._probe, probe_url, proxy, deadline, probe_options)
                    probes[future] = proxy
                    future.add_done_callback(completed.put)
                attempts += len(probes)
                for _ in probes:
                    future = completed.get(timeout=deadline.remaining())
                    proxy = probes[future]
                    try:
                        future.result()
                    except (_transport.ProxyTransportError, requests.exceptions.SSLError) as exc:
                        self._pool.record(origin, proxy, None)
                        last_error = exc
                        continue
                    return proxy, attempts
        except Empty as exc:
            last_error = exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        message = (
            "Proxy request exceeded its time budget during preflight"
            if not deadline.remaining()
            else f"No proxy passed preflight ({attempts} candidates)"
        )
        raise ProxyRequestError(message, attempts=attempts) from last_error

    def request(
        self,
        method: str,
        url: str,
        *,
        country: str | None = None,
        protocol: str | None = None,
        timeout: float = 15,
        max_attempts: int | None = None,
        retry_unsafe: bool = False,
        preflight: bool = False,
        **options: Any,
    ) -> requests.Response:
        """Send a buffered request through one or more eligible proxies.

        With preflight enabled, probe candidates with anonymous HEAD requests in
        groups limited by preflight_concurrency. Send the original request once
        through the first passing proxy.
        Accept 2xx, 3xx, 401, 404 and 405 from the probe. Both operations share
        the timeout, and max_attempts limits the number of candidates probed.
        """
        if max_attempts is None:
            max_attempts = self._preflight_max_attempts if preflight else 3
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
        if preflight:
            parsed = urlsplit(url)
            if parsed.scheme != "https" or parsed.username is not None or parsed.password is not None:
                raise ValueError("preflight requires an HTTPS URL without user information")
        filters = self._filters(country, protocol)
        deadline = Deadline.after(positive_seconds(timeout, "timeout"))
        # Retry buffered bodies only. File handles and iterators may have been
        # consumed by the previous attempt.
        body = options.get("data")
        replayable = not options.get("files") and (
            body is None or isinstance(body, (bytes, str))
        )
        if not preflight and (not replayable or (method not in _SAFE_METHODS and not retry_unsafe)):
            max_attempts = 1

        proxies = self._pool.ranked(self._pool.snapshot(filters, deadline), origin, prefer_measured=not preflight)
        if not proxies:
            raise NoProxyAvailable(
                f"No eligible proxy for country={filters.country!r}, protocol={filters.protocol!r}"
            )
        attempts = 0
        proxies = proxies[:max_attempts]
        if preflight:
            selected, attempts = self._select_proxy(url, proxies, origin, deadline, options)
            proxies = [selected]
        last_error: Exception | None = None
        for proxy in proxies:
            remaining = deadline.remaining()
            if not remaining:
                break
            if not preflight:
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
        if not deadline.remaining():
            message = "Proxy request exceeded its time budget"
        elif preflight:
            message = "Proxy request failed after preflight and was not retried"
        else:
            message = "Proxy request failed on all attempted proxies"
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
