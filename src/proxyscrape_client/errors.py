"""Errors raised by the proxy selection and retry layers."""


class ProxyScrapeError(Exception):
    """Base error for the client."""


class ProxySourceError(ProxyScrapeError):
    """ProxyScrape could not provide a usable snapshot."""


class NoProxyAvailable(ProxyScrapeError):
    """No eligible proxy is available for the current filters and target."""


class ProxyRequestError(ProxyScrapeError):
    """The request could not be completed within its attempt or time budget."""

    def __init__(self, message: str, *, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts
