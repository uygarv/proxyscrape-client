import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from unittest.mock import patch

from proxyscrape_client import Client, NoProxyAvailable, Proxy, ProxyRequestError, ProxySourceError
from proxyscrape_client._transport import ProxyTransportError


def candidate(port, uptime, country="tr"):
    return Proxy(f"http://127.0.0.1:{port}", "http", country, uptime)


@contextmanager
def local_proxy(status):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(status)
            self.end_headers()
            self.wfile.write(b"proxy response")

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield server.server_port, calls
        finally:
            server.shutdown()
            worker.join()


class ClientTests(unittest.TestCase):
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_proxy_fallback_and_target_specific_health(self, source):
        with local_proxy(407) as (bad, failures), local_proxy(200) as (good, successes):
            source.return_value.fetch.return_value = (candidate(bad, 99), candidate(good, 90))
            with Client(country="TR") as client:
                response = client.get("http://example.test/page")
                self.assertEqual(response.text, "proxy response")
                client.get("http://example.test/again")
                client.get("http://another.test/")
                self.assertEqual(failures, ["http://example.test/page", "http://another.test/"])
                self.assertEqual(len(successes), 3)
            self.assertEqual(source.return_value.fetch.call_count, 1)
            self.assertEqual(source.return_value.fetch.call_args.args[0].country, "tr")

    @patch("proxyscrape_client.client._transport.send")
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_attempt_limits_and_unsafe_opt_in(self, source, send):
        source.return_value.fetch.return_value = (candidate(8001, 99), candidate(8002, 90))
        send.side_effect = ProxyTransportError("failed")
        with Client() as client:
            with self.assertRaises(ProxyRequestError) as single:
                client.post("https://example.test/", json={"test": True})
            self.assertEqual(single.exception.attempts, 1)
            with self.assertRaises(ProxyRequestError) as multiple:
                client.post("https://another.test/", retry_unsafe=True, json={"test": True})
            self.assertEqual(multiple.exception.attempts, 2)
            with self.assertRaises(ProxyRequestError) as limited:
                client.get("https://third.test/", max_attempts=1)
            self.assertEqual(limited.exception.attempts, 1)
            self.assertEqual(send.call_count, 4)

    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_empty_country_does_not_use_other_cached_country(self, source):
        source.return_value.fetch.side_effect = lambda filters, _: (
            (candidate(8001, 99, "de"),) if filters.country == "de" else ()
        )
        with Client(country="tr") as client:
            self.assertEqual(len(client.list_proxies(country="DE")), 1)
            self.assertEqual(client.list_proxies(), [])
            with self.assertRaises(NoProxyAvailable):
                client.get("https://example.test/")

    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_concurrent_list_calls_share_refresh(self, source):
        started = Event()
        release = Event()

        def fetch(*_):
            started.set()
            if not release.wait(2):
                raise AssertionError("test did not release refresh")
            return (candidate(8001, 99),)

        source.return_value.fetch.side_effect = fetch
        with Client() as client, ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(client.list_proxies)
            self.assertTrue(started.wait(2))
            second = executor.submit(client.list_proxies)
            release.set()
            self.assertEqual(first.result(2), second.result(2))
        source.return_value.fetch.assert_called_once()

    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_background_refresh_keeps_stale_snapshot_after_source_failure(self, source):
        refreshed = Event()
        proxy = candidate(8001, 99)

        def fail(*_):
            refreshed.set()
            raise ProxySourceError("source down")

        fetch = source.return_value.fetch
        fetch.side_effect = lambda *_: (proxy,)
        with Client(refresh_interval=0.02) as client:
            self.assertEqual(client.list_proxies(), [proxy])
            fetch.side_effect = fail
            self.assertTrue(refreshed.wait(2))
            self.assertEqual(client.list_proxies(), [proxy])
        with self.assertRaises(RuntimeError):
            client.list_proxies()

    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_cold_source_failure(self, source):
        source.return_value.fetch.side_effect = ProxySourceError("source down")
        with Client() as client, self.assertRaises(ProxySourceError):
            client.list_proxies()
