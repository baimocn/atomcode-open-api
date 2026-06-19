"""
test_health_checker - SlidingWindowStats / GatewayHealthChecker / GatewayPool
"""

import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from atomcode_open_api.health_checker import (  # noqa: E402
    GatewayHealthChecker,
    GatewayPool,
    SlidingWindowStats,
)


# ── 一个可控的本地 fake gateway HTTP 服务，用于探测测试 ────────────

class _FakeGatewayHandler(BaseHTTPRequestHandler):
    # 由测试在类上注入
    status_to_return: int = 200
    delay_seconds: float = 0.0

    def do_GET(self):
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        self.send_response(self.status_to_return)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"data": []}')

    def log_message(self, *a, **kw):
        return


def _start_fake(status: int = 200, delay: float = 0.0):
    class _H(_FakeGatewayHandler):
        pass

    _H.status_to_return = status
    _H.delay_seconds = delay
    server = HTTPServer(("127.0.0.1", 0), _H)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port, thread


def _stop_fake(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


# ── SlidingWindowStats ──────────────────────────────────────────────

class TestSlidingWindowStats(unittest.TestCase):
    def test_initial_empty(self):
        s = SlidingWindowStats(window_size=5)
        self.assertEqual(s.sample_count(), 0)
        self.assertEqual(s.success_rate(), 0.0)
        self.assertIsNone(s.avg_latency())
        self.assertFalse(s.is_healthy())

    def test_record_and_metrics(self):
        s = SlidingWindowStats(window_size=10)
        s.record(100, True)
        s.record(200, True)
        s.record(50, False)
        self.assertEqual(s.sample_count(), 3)
        self.assertAlmostEqual(s.success_rate(), 2 / 3, places=4)
        # avg 只统计成功
        self.assertAlmostEqual(s.avg_latency(), 150.0, places=2)

    def test_window_size_drops_old(self):
        s = SlidingWindowStats(window_size=3)
        for i in range(5):
            s.record(i * 100, True)
        # 只保留最新 3 条：200,300,400
        self.assertEqual(s.sample_count(), 3)
        self.assertAlmostEqual(s.avg_latency(), 300.0, places=2)

    def test_is_healthy(self):
        s = SlidingWindowStats(window_size=10)
        for _ in range(8):
            s.record(100, True)
        for _ in range(2):
            s.record(100, False)
        self.assertTrue(s.is_healthy(threshold=0.5, max_latency_ms=5000))
        # 提高阈值
        self.assertFalse(s.is_healthy(threshold=0.9, max_latency_ms=5000))
        # 收紧延迟
        self.assertFalse(s.is_healthy(threshold=0.5, max_latency_ms=50))


# ── GatewayHealthChecker ────────────────────────────────────────────

class TestGatewayHealthChecker(unittest.TestCase):
    def test_check_returns_healthy_when_2xx(self):
        server, port, thread = _start_fake(status=200)
        try:
            hc = GatewayHealthChecker(probe_url="/v1/models", interval=10, timeout=2)
            res = hc.check(f"http://127.0.0.1:{port}", access_token="tok")
            self.assertTrue(res["healthy"])
            self.assertEqual(res["status_code"], 200)
            self.assertGreaterEqual(res["latency_ms"], 0)
            self.assertIsNone(res["error"])
        finally:
            _stop_fake(server, thread)

    def test_check_unhealthy_on_5xx(self):
        server, port, thread = _start_fake(status=503)
        try:
            hc = GatewayHealthChecker(interval=10, timeout=2)
            res = hc.check(f"http://127.0.0.1:{port}", access_token="tok")
            self.assertFalse(res["healthy"])
            self.assertEqual(res["status_code"], 503)
        finally:
            _stop_fake(server, thread)

    def test_check_unhealthy_on_network_error(self):
        # 用一个保证未监听的端口
        hc = GatewayHealthChecker(interval=10, timeout=1)
        res = hc.check("http://127.0.0.1:1", access_token="tok")
        self.assertFalse(res["healthy"])
        self.assertIsNotNone(res["error"])
        self.assertEqual(res["latency_ms"], -1)

    def test_start_stop_calls_callback_periodically(self):
        hc = GatewayHealthChecker(interval=0.1, timeout=1)
        counter = {"n": 0}
        ev = threading.Event()

        def cb():
            counter["n"] += 1
            if counter["n"] >= 3:
                ev.set()

        hc.start(cb)
        try:
            self.assertTrue(ev.wait(timeout=3.0),
                            "callback was not called enough times")
        finally:
            hc.stop(timeout=2.0)
        self.assertGreaterEqual(counter["n"], 3)


# ── GatewayPool ─────────────────────────────────────────────────────

class TestGatewayPool(unittest.TestCase):
    def _make_pool(self, urls):
        hc = GatewayHealthChecker(interval=60, timeout=1)
        return GatewayPool(gateway_urls=urls, access_token="tok",
                            health_checker=hc), hc

    def test_select_picks_lowest_latency_healthy(self):
        urls = ["http://a", "http://b", "http://c"]
        pool, _ = self._make_pool(urls)

        # 让 a/b/c 都进入"健康"区间，但 b 延迟最低
        for _ in range(5):
            pool.report_result("http://a", 300, True)
            pool.report_result("http://b", 100, True)
            pool.report_result("http://c", 200, True)

        chosen = pool.select()
        self.assertEqual(chosen, "http://b")

    def test_select_degraded_mode(self):
        urls = ["http://a", "http://b"]
        pool, _ = self._make_pool(urls)
        # 全部不健康（很高延迟+失败混合）
        for _ in range(10):
            pool.report_result("http://a", 9000, False)
        for _ in range(10):
            pool.report_result("http://b", 9000, False)
        # b 偶尔成功，使其成功率更高
        for _ in range(2):
            pool.report_result("http://b", 9000, True)

        chosen = pool.select()
        self.assertEqual(chosen, "http://b")

    def test_mark_down_excluded(self):
        urls = ["http://a", "http://b"]
        pool, _ = self._make_pool(urls)
        # 让两个都健康
        for _ in range(5):
            pool.report_result("http://a", 100, True)
            pool.report_result("http://b", 200, True)
        # mark a 为 down
        pool.mark_down("http://a", cooldown=10.0)
        chosen = pool.select()
        self.assertEqual(chosen, "http://b")

    def test_empty_pool_raises(self):
        with self.assertRaises(ValueError):
            GatewayPool([], access_token="x",
                        health_checker=GatewayHealthChecker())

    def test_probe_all_records_stats(self):
        server, port, thread = _start_fake(status=200)
        try:
            url = f"http://127.0.0.1:{port}"
            urls = [url]
            hc = GatewayHealthChecker(probe_url="/v1/models",
                                      interval=60, timeout=2)
            pool = GatewayPool(gateway_urls=urls, access_token="tok",
                                health_checker=hc)
            pool.probe_all()
            status = pool.status()
            self.assertEqual(status["gateways"][0]["samples"], 1)
            self.assertTrue(status["gateways"][0]["healthy"])
        finally:
            _stop_fake(server, thread)

    def test_get_gateway_returns_instance(self):
        urls = ["http://a", "http://b"]
        pool, _ = self._make_pool(urls)
        gw = pool.get_gateway("http://a")
        self.assertEqual(gw.gateway_url, "http://a")
        # 末尾斜杠也能查到
        gw2 = pool.get_gateway("http://a/")
        self.assertIs(gw2, gw)

    def test_status_shape(self):
        urls = ["http://a"]
        pool, _ = self._make_pool(urls)
        pool.report_result("http://a", 100, True)
        st = pool.status()
        self.assertIn("current", st)
        self.assertIn("gateways", st)
        g = st["gateways"][0]
        for k in ("url", "healthy", "success_rate", "avg_latency_ms",
                  "samples", "down_until_in"):
            self.assertIn(k, g)


if __name__ == "__main__":
    unittest.main()
