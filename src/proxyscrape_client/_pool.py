from collections import OrderedDict
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from time import monotonic

from ._provider import ProxySource
from .errors import ProxySourceError
from .models import Deadline, Filters, Proxy

Origin = tuple[str, str, int]


@dataclass
class _Snapshot:
    lock: Lock = field(default_factory=Lock)
    proxies: tuple[Proxy, ...] = ()
    updated_at: float = float("-inf")
    next_refresh: float = 0
    error: ProxySourceError | None = None


@dataclass
class _Health:
    latency: float | None = None
    blocked_until: float = 0


class ProxyPool:
    def __init__(
        self,
        source: ProxySource,
        refresh_interval: float,
        max_stale: float,
        source_timeout: float,
        cooldown: float,
    ) -> None:
        self._source = source
        self._refresh_interval = refresh_interval
        self._max_stale = max_stale
        self._source_timeout = source_timeout
        self._cooldown = cooldown
        self._lock = Lock()
        self._stop = Event()
        self._worker: Thread | None = None
        self._snapshots: dict[Filters, _Snapshot] = {}
        self._health: OrderedDict[tuple[Origin, str], _Health] = OrderedDict()

    def snapshot(self, filters: Filters, deadline: Deadline) -> tuple[Proxy, ...]:
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("ProxyScrape client is closed")
            snapshot = self._snapshots.get(filters)
            if snapshot is None:
                snapshot = self._snapshots[filters] = _Snapshot()
            if self._worker is None:
                self._worker = Thread(target=self._run, name="proxyscrape-refresh", daemon=True)
                self._worker.start()
        return self._refresh(filters, snapshot, deadline)

    def _refresh(
        self, filters: Filters, snapshot: _Snapshot, deadline: Deadline
    ) -> tuple[Proxy, ...]:
        remaining = deadline.remaining()
        if not remaining or not snapshot.lock.acquire(timeout=remaining):
            raise ProxySourceError("Timed out waiting for a ProxyScrape snapshot")
        try:
            now = monotonic()
            if now >= snapshot.next_refresh:
                source_deadline = Deadline(min(deadline.expires_at, now + self._source_timeout))
                try:
                    proxies = self._source.fetch(filters, source_deadline)
                except ProxySourceError as exc:
                    snapshot.error = exc
                    snapshot.next_refresh = monotonic() + min(5.0, self._refresh_interval)
                else:
                    snapshot.proxies = proxies
                    snapshot.updated_at = monotonic()
                    snapshot.next_refresh = snapshot.updated_at + self._refresh_interval
                    snapshot.error = None

            if snapshot.error is not None and monotonic() - snapshot.updated_at >= self._max_stale:
                message = "No current ProxyScrape snapshot for the requested filters"
                raise ProxySourceError(message) from snapshot.error
            return snapshot.proxies
        finally:
            snapshot.lock.release()

    def _run(self) -> None:
        # Poll separately from snapshot expiration so a foreground refresh does
        # not postpone the next background refresh by a full interval.
        while not self._stop.wait(min(1.0, self._refresh_interval)):
            with self._lock:
                snapshots = list(self._snapshots.items())
            for filters, snapshot in snapshots:
                if self._stop.is_set():
                    return
                if monotonic() < snapshot.next_refresh:
                    continue
                try:
                    self._refresh(filters, snapshot, Deadline.after(self._source_timeout))
                except ProxySourceError:
                    continue

    def ranked(
        self, proxies: tuple[Proxy, ...], origin: Origin | None = None, *, prefer_measured: bool = True
    ) -> list[Proxy]:
        now = monotonic()
        with self._lock:
            ranked = []
            for proxy in proxies:
                health = self._health.get((origin, proxy.url)) if origin is not None else None
                if health is not None and health.blocked_until > now:
                    continue
                measured = health.latency if health is not None and prefer_measured else None
                rank = (
                    measured is None,
                    measured if measured is not None else 0.0,
                    origin is not None and origin[0] == "https" and proxy.supports_https is False,
                    -(proxy.uptime if proxy.uptime is not None else -1),
                    proxy.latency_ms if proxy.latency_ms is not None else float("inf"),
                    proxy.url,
                )
                ranked.append((rank, proxy))
        return [proxy for _, proxy in sorted(ranked, key=lambda item: item[0])]

    def record(self, origin: Origin, proxy: Proxy, latency: float | None) -> None:
        key = (origin, proxy.url)
        with self._lock:
            if latency is None:
                health = _Health(blocked_until=monotonic() + self._cooldown)
            else:
                previous = self._health.get(key)
                if previous is not None and previous.latency is not None:
                    latency = 0.75 * previous.latency + 0.25 * latency
                health = _Health(latency=latency)
            self._health[key] = health
            self._health.move_to_end(key)
            if len(self._health) > 4096:
                self._health.popitem(last=False)

    def close(self) -> None:
        with self._lock:
            self._stop.set()
            worker = self._worker
        if worker is not None:
            worker.join()
