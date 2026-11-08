from dataclasses import dataclass
from math import isfinite
from time import monotonic

PROTOCOLS = frozenset({"http", "https", "socks4", "socks5"})


def positive_seconds(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number of seconds")
    if not isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number of seconds")
    return float(value)


@dataclass(frozen=True)
class Proxy:
    """A ProxyScrape candidate and its source-reported metrics."""

    url: str
    protocol: str
    country: str
    uptime: float | None = None
    latency_ms: float | None = None
    supports_https: bool | None = None


@dataclass(frozen=True)
class Filters:
    country: str | None
    protocol: str | None

    def __post_init__(self) -> None:
        if self.country is not None:
            country = self.country.strip().lower()
            if len(country) != 2 or not country.isascii() or not country.isalpha():
                raise ValueError("country must be a two-letter country code")
            object.__setattr__(self, "country", country)
        if self.protocol is not None:
            protocol = self.protocol.strip().lower()
            if protocol not in PROTOCOLS:
                raise ValueError(f"protocol must be one of {', '.join(sorted(PROTOCOLS))}")
            object.__setattr__(self, "protocol", protocol)


@dataclass(frozen=True)
class Deadline:
    expires_at: float

    @classmethod
    def after(cls, seconds: float) -> "Deadline":
        return cls(monotonic() + seconds)

    def remaining(self) -> float:
        return max(0.0, self.expires_at - monotonic())
