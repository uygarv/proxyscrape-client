import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Barrier, Event, Thread
from unittest.mock import patch

from requests import Response

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

    @patch("proxyscrape_client.client._transport.send")
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_preflight_skips_failed_candidates_and_uses_the_passing_proxy_once(self, source, send):
        first_batch = tuple(candidate(8000 + index, 100 - index) for index in range(3))
        working = candidate(8003, 90)
        source.return_value.fetch.return_value = (working, *first_batch)
        started = Barrier(3)

        def respond(method, url, proxy, timeout, options):
            if method == "POST":
                raise ProxyTransportError("lost reply")
            if proxy in first_batch:
                started.wait(2)
                if proxy != first_batch[1]:
                    raise ProxyTransportError("offline")
            reply = Response()
            reply.status_code = 405 if proxy == working else 403
            reply._content_consumed = True
            return reply

        send.side_effect = respond
        url = "https://example.test/refresh?secret=hidden"
        with Client(preflight_method="OPTIONS") as client:
            client._pool.record(("https", "example.test", 443), working, 0.1)
            with self.assertRaisesRegex(ProxyRequestError, "failed after preflight"):
                client.post(url, preflight=True, headers={"Authorization": "Bearer example"}, json={"token": "example"})
        self.assertCountEqual([call.args[2] for call in send.call_args_list[:3]], first_batch)
        self.assertEqual(
            [(call.args[0], call.args[2]) for call in send.call_args_list[3:]],
            [("OPTIONS", working), ("POST", working)],
        )
        for call in send.call_args_list[:4]:
            self.assertEqual(call.args[0], "OPTIONS")
            self.assertEqual(call.args[1], "https://example.test/refresh")
            self.assertEqual(call.args[4], {"allow_redirects": False, "verify": True, "headers": {}})
        self.assertEqual(send.call_args.args[1], url)
        self.assertEqual(send.call_args.args[4]["json"], {"token": "example"})

    @patch("proxyscrape_client.client._transport.send")
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_preflight_exhaustion_sends_no_post(self, source, send):
        source.return_value.fetch.return_value = (candidate(8001, 99), candidate(8002, 95), candidate(8003, 90))
        send.side_effect = ProxyTransportError("offline")
        with Client() as client:
            with self.assertRaisesRegex(ProxyRequestError, "No proxy passed preflight") as failure:
                client.post("https://example.test/refresh", preflight=True, max_attempts=2, data=b"private")
        self.assertEqual(failure.exception.attempts, 2)
        self.assertEqual([call.args[0] for call in send.call_args_list], ["HEAD", "HEAD"])

    @patch("proxyscrape_client.client._transport.send")
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_preflight_sends_post_without_waiting_for_slower_probes(self, source, send):
        first, winner, third, unused = tuple(candidate(8000 + index, 100 - index) for index in range(4))
        source.return_value.fetch.return_value = (first, winner, third, unused)
        started = Barrier(3)
        post_started = Event()

        def respond(method, url, proxy, timeout, options):
            if method == "HEAD":
                started.wait(2)
                if proxy != winner:
                    if not post_started.wait(2):
                        raise AssertionError("POST waited for the slower probes")
                    raise ProxyTransportError("offline")
            else:
                post_started.set()
            reply = Response()
            reply.status_code = 200
            reply._content_consumed = True
            return reply

        send.side_effect = respond
        with Client() as client:
            with client.post("https://example.test/refresh", preflight=True) as response:
                self.assertEqual(response.status_code, 200)
        self.assertCountEqual([call.args[2] for call in send.call_args_list[:3]], (first, winner, third))
        self.assertEqual([(call.args[0], call.args[2]) for call in send.call_args_list[3:]], [("POST", winner)])

    @patch("proxyscrape_client.client._transport.send", side_effect=ProxyTransportError("offline"))
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_preflight_tries_at_most_nine_candidates_by_default(self, source, send):
        source.return_value.fetch.return_value = tuple(candidate(8000 + index, 100 - index) for index in range(12))
        with Client() as client:
            with self.assertRaisesRegex(ProxyRequestError, "9 candidates"):
                client.post("https://example.test/refresh", preflight=True)
        self.assertEqual(send.call_count, 9)

    @patch("proxyscrape_client.client._transport.send")
    @patch("proxyscrape_client.client.ProxyScrapeSource")
    def test_preflight_uses_custom_batch_size_and_candidate_limit(self, source, send):
        proxies = tuple(candidate(8000 + index, 100 - index) for index in range(6))
        source.return_value.fetch.return_value = proxies
        started = Barrier(2)

        def fail(method, url, proxy, timeout, options):
            if proxy in proxies[:4]:
                started.wait(2)
            raise ProxyTransportError("offline")

        send.side_effect = fail
        with Client(preflight_concurrency=2, preflight_max_attempts=5) as client:
            with self.assertRaisesRegex(ProxyRequestError, "5 candidates"):
                client.post("https://example.test/refresh", preflight=True)
        calls = [call.args[2] for call in send.call_args_list]
        self.assertCountEqual(calls[:2], proxies[:2])
        self.assertCountEqual(calls[2:4], proxies[2:4])
        self.assertEqual(calls[4:], [proxies[4]])
        self.assertTrue(all(call.args[0] == "HEAD" for call in send.call_args_list))

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
