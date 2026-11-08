import unittest
from unittest.mock import patch

from proxyscrape_client._provider import ProxyScrapeSource
from proxyscrape_client.errors import ProxySourceError
from proxyscrape_client.models import Deadline, Filters


def record(ip="8.8.8.8", country="TR", protocol="http"):
    return {
        "alive": True,
        "protocol": protocol,
        "ip": ip,
        "port": 3128,
        "ip_data": {"countryCode": country},
        "uptime": 96,
        "average_timeout": 155.0,
        "ssl": True,
    }


class SourceTests(unittest.TestCase):
    @patch("proxyscrape_client._provider.requests.Session")
    def test_pagination_deduplication_and_strict_filters(self, session):
        valid = record()
        pages = {
            0: {"skip": 0, "limit": 2, "nextpage": True, "proxies": [valid, record(country="DE")]},
            2: {
                "skip": 2,
                "nextpage": False,
                "proxies": [valid, record(ip="127.0.0.1"), record(protocol="socks5")],
            },
        }
        calls = []
        http = session.return_value.__enter__.return_value

        def get(_, *, params, timeout):
            calls.append(params.copy())
            response = unittest.mock.Mock()
            response.json.return_value = pages[params["skip"]]
            return response

        http.get.side_effect = get
        proxies = ProxyScrapeSource().fetch(Filters("TR", "http"), Deadline.after(1))
        self.assertEqual(len(proxies), 1)
        self.assertEqual(proxies[0].url, "http://8.8.8.8:3128")
        self.assertEqual(proxies[0].latency_ms, 155.0)
        self.assertEqual([p["skip"] for p in calls], [0, 2])
        self.assertEqual([p["country"] for p in calls], ["tr", "tr"])

    @patch("proxyscrape_client._provider.requests.Session")
    def test_invalid_source_response_is_not_an_empty_list(self, session):
        http = session.return_value.__enter__.return_value
        http.get.return_value.json.return_value = {"error": "unavailable"}
        with self.assertRaises(ProxySourceError):
            ProxyScrapeSource().fetch(Filters("tr", None), Deadline.after(1))
