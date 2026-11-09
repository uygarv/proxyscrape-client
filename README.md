# proxyscrape-client

Send HTTP requests through a pool of free public proxies from ProxyScrape, without managing the proxy list yourself.

The client refreshes the list in the background. If a request fails to connect, it tries another proxy for you.

## Install

Python 3.10 or later is required.

```sh
git clone https://github.com/uygarv/proxyscrape-client.git
cd proxyscrape-client
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

## Try it

```python
from proxyscrape_client import Client

with Client() as client:
    response = client.get("https://example.com/")
    response.raise_for_status()
    print(response.status_code)
```

## Choosing proxies

Two filters control which proxies you get:

- `country`: a two letter country code, like `"DE"` or `"US"`.
- `protocol`: one of `http`, `https`, `socks4`, or `socks5`.

Set them on the client to make them the default, for example `Client(country="DE", protocol="socks5")`. You can also pass either one to `get()` or `list_proxies()` to override the default for a single call.

`list_proxies()` returns matching `Proxy` objects, ranked by the uptime and latency that ProxyScrape reports. Each one has a `url` and a `protocol`, and a `latency_ms` that can be `None` when it isn't known. If nothing matches, you get an empty list.

## Requests and retries

`get()`, `head()`, `post()`, and `request()` all return a regular `requests.Response`, already buffered. Call `raise_for_status()` if you want HTTP errors raised.

Retries depend on the method:

- `GET` and `HEAD` try up to three proxies by default when a proxy fails to connect or rejects authentication.
- Everything else gets a single attempt by default.
- If you have a request that can be replayed and it's safe to send more than once, pass `retry_unsafe=True` to let it retry too.

You can change the number of tries per call with `max_attempts`.

### Errors

Everything the library raises on purpose inherits from `ProxyScrapeError`, so you can catch the class to cover.

- `ProxySourceError`: the client couldn't get a usable proxy list.
- `NoProxyAvailable`: no eligible proxy was available.
- `ProxyRequestError`: the request ran out of attempts or time. Its `attempts` attribute tells you how many proxies were tried.

## Single-shot requests with preflight

Some HTTPS requests must not be sent twice, such as a payment or any non-idempotent call. Pass `preflight=True` to make sure the request goes out exactly once, through a candidate that has already been checked.

### How it works

1. **Rank.** Candidates are ordered by source-reported HTTPS support, uptime, and latency. The target's cooldown is respected.
2. **Probe.** Anonymous `HEAD` probes are sent in batches. The client moves on to the next batch only if every candidate in the current batch fails.
3. **Send.** The first candidate to pass receives the original request, once.

### Configuration

| Option | Where | Default | Description |
|---|---|---|---|
| `preflight_concurrency` | `Client` | `3` | Number of candidates probed per batch |
| `preflight_max_attempts` | `Client` | `9` | Total number of candidates to try |
| `max_attempts` | per request | `preflight_max_attempts` | Overrides the candidate limit for a single request |
| `timeout` | per request | n/a | Bounds both the probing phase and the original request |

### Example

```python
client = Client(preflight_concurrency=5, preflight_max_attempts=12)

# Uses the client-level limits
client.post(url, data=payload, preflight=True)

# Tighter limits for this request only
client.post(url, data=payload, preflight=True, max_attempts=4, timeout=10)
```

## Examples

### List SOCKS5 proxies in a country

This prints the first five SOCKS5 proxies in Germany along with their reported latency.

```python
from proxyscrape_client import Client, Proxy

with Client(protocol="socks5") as client:
    proxies: list[Proxy] = client.list_proxies(country="DE")

for proxy in proxies[:5]:
    latency = f"{proxy.latency_ms:g} ms" if proxy.latency_ms is not None else "unknown"
    print(f"{proxy.url} | latency: {latency}")
```

### Override the country for one request

Here the client defaults to German proxies, but this one request goes through a US proxy. The `protocol="http"` setting describes how the client talks to the proxy, and it stays the same.

```python
from proxyscrape_client import Client, ProxyRequestError, ProxyScrapeError

with Client(country="DE", protocol="http") as client:
    try:
        response = client.get("https://example.com/", country="US", max_attempts=3)
    except ProxyRequestError as error:
        print(f"Request failed after {error.attempts} proxy attempts: {error}")
    except ProxyScrapeError as error:
        print(f"Could not select a proxy: {error}")
    else:
        response.raise_for_status()
        print(response.status_code)
```

### Send a JSON POST

Pass the target URL as the first command line argument. `POST` isn't retried by default, so a failed attempt won't send the same payload twice.

```python
import sys

from proxyscrape_client import Client

with Client() as client:
    response = client.post(sys.argv[1], json={"event": "demo"}, timeout=10)
    response.raise_for_status()
    print(response.status_code)
```

## License

Released under the [MIT License](LICENSE).
