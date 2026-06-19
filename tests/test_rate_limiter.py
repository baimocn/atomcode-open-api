"""
test_rate_limiter - TokenBucket / RequestQueue / RateLimiter 的单元测试
"""

import os
import sys
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from atomcode_open_api.rate_limiter import (  # noqa: E402
    RateLimiter,
    RequestQueue,
    TokenBucket,
)


# ── TokenBucket ─────────────────────────────────────────────────────

class TestTokenBucket(unittest.TestCase):
    def test_init_fills_to_capacity(self):
        b = TokenBucket(rate=5.0, capacity=10)
        self.assertAlmostEqual(b.token_count(), 10.0, places=1)

    def test_acquire_decrements(self):
        b = TokenBucket(rate=0.0, capacity=5)
        self.assertTrue(b.acquire(1))
        self.assertTrue(b.acquire(2))
        self.assertAlmostEqual(b.token_count(), 2.0, places=1)

    def test_acquire_fails_when_empty(self):
        b = TokenBucket(rate=0.0, capacity=2)
        self.assertTrue(b.acquire(2))
        self.assertFalse(b.acquire(1, timeout=0))

    def test_refill_over_time(self):
        b = TokenBucket(rate=10.0, capacity=10)
        # 用完
        self.assertTrue(b.acquire(10))
        self.assertFalse(b.acquire(1, timeout=0))
        # 等 0.25 秒应补回 ~2.5 个
        time.sleep(0.3)
        self.assertGreaterEqual(b.token_count(), 2.0)

    def test_refill_capped_at_capacity(self):
        b = TokenBucket(rate=100.0, capacity=3)
        time.sleep(0.2)
        self.assertLessEqual(b.token_count(), 3.0)

    def test_acquire_with_timeout_waits(self):
        b = TokenBucket(rate=20.0, capacity=1)
        self.assertTrue(b.acquire(1))
        # 立即失败
        self.assertFalse(b.acquire(1, timeout=0))
        # 等待时应在 0.5 秒内拿到（20 token/s -> 50ms）
        start = time.monotonic()
        ok = b.acquire(1, timeout=0.5)
        elapsed = time.monotonic() - start
        self.assertTrue(ok)
        self.assertLess(elapsed, 0.5)

    def test_acquire_more_than_capacity_returns_false(self):
        b = TokenBucket(rate=10.0, capacity=2)
        self.assertFalse(b.acquire(5, timeout=0.1))

    def test_thread_safety(self):
        """两个线程并发各 acquire 1000 次 1 个令牌，总数不应超过被填充的量。"""
        b = TokenBucket(rate=0.0, capacity=1000)
        results = []
        results_lock = threading.Lock()

        def worker():
            local = 0
            for _ in range(1000):
                if b.acquire(1, timeout=0):
                    local += 1
            with results_lock:
                results.append(local)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 总成功次数恰好等于 capacity
        self.assertEqual(sum(results), 1000)


# ── RequestQueue ────────────────────────────────────────────────────

class TestRequestQueue(unittest.TestCase):
    def test_enqueue_dequeue_basic(self):
        q = RequestQueue(max_size=10)
        self.assertTrue(q.enqueue("a", priority=5))
        self.assertTrue(q.enqueue("b", priority=1))
        self.assertTrue(q.enqueue("c", priority=3))
        # 优先级 1 最小，应先出
        self.assertEqual(q.dequeue(), "b")
        self.assertEqual(q.dequeue(), "c")
        self.assertEqual(q.dequeue(), "a")
        self.assertIsNone(q.dequeue())

    def test_fifo_within_same_priority(self):
        q = RequestQueue(max_size=10)
        for rid in ["x", "y", "z"]:
            q.enqueue(rid, priority=0)
        self.assertEqual(q.dequeue(), "x")
        self.assertEqual(q.dequeue(), "y")
        self.assertEqual(q.dequeue(), "z")

    def test_max_size(self):
        q = RequestQueue(max_size=2)
        self.assertTrue(q.enqueue("a"))
        self.assertTrue(q.enqueue("b"))
        self.assertFalse(q.enqueue("c"))
        self.assertEqual(q.pending_count(), 2)

    def test_pending_count(self):
        q = RequestQueue(max_size=5)
        self.assertEqual(q.pending_count(), 0)
        q.enqueue("a")
        q.enqueue("b")
        self.assertEqual(q.pending_count(), 2)
        q.dequeue()
        self.assertEqual(q.pending_count(), 1)

    def test_is_expired(self):
        q = RequestQueue(max_size=5, timeout=0.1)
        q.enqueue("a")
        self.assertFalse(q.is_expired("a"))
        time.sleep(0.15)
        self.assertTrue(q.is_expired("a"))
        # 不在队列中的 id：返回 False
        self.assertFalse(q.is_expired("nonexistent"))

    def test_remove_expired(self):
        q = RequestQueue(max_size=5, timeout=0.1)
        q.enqueue("old1")
        q.enqueue("old2")
        time.sleep(0.15)
        q.enqueue("new")
        removed = q.remove_expired()
        self.assertEqual(set(removed), {"old1", "old2"})
        self.assertEqual(q.pending_count(), 1)
        self.assertEqual(q.dequeue(), "new")

    def test_duplicate_enqueue_rejected(self):
        q = RequestQueue(max_size=5)
        self.assertTrue(q.enqueue("a"))
        self.assertFalse(q.enqueue("a"))


# ── RateLimiter ─────────────────────────────────────────────────────

class TestRateLimiter(unittest.TestCase):
    def test_try_acquire_allowed(self):
        rl = RateLimiter(rate=0.0, capacity=3, queue_size=10)
        d = rl.try_acquire()
        self.assertTrue(d["allowed"])
        self.assertFalse(d["queued"])

    def test_try_acquire_queued(self):
        rl = RateLimiter(rate=0.0, capacity=1, queue_size=5)
        # 第一次：用掉唯一令牌
        first = rl.try_acquire()
        self.assertTrue(first["allowed"])
        # 第二次：令牌没了，应被入队
        second = rl.try_acquire()
        self.assertFalse(second["allowed"])
        self.assertTrue(second["queued"])
        self.assertEqual(second["queue_position"], 1)
        self.assertIsNotNone(second["request_id"])

    def test_try_acquire_rejected_when_queue_full(self):
        rl = RateLimiter(rate=0.0, capacity=1, queue_size=2)
        rl.try_acquire()  # allowed
        rl.try_acquire()  # queued #1
        rl.try_acquire()  # queued #2
        d = rl.try_acquire()  # rejected
        self.assertFalse(d["allowed"])
        self.assertFalse(d["queued"])

    def test_process_queue_returns_next(self):
        rl = RateLimiter(rate=100.0, capacity=1, queue_size=5)
        rl.try_acquire()  # allowed, bucket=0
        queued = rl.try_acquire()  # queued
        self.assertTrue(queued["queued"])

        # 等待令牌再生
        time.sleep(0.05)
        rid = rl.process_queue()
        self.assertEqual(rid, queued["request_id"])

    def test_process_queue_removes_expired(self):
        rl = RateLimiter(rate=0.0, capacity=1, queue_size=5, queue_timeout=0.1)
        rl.try_acquire()  # allowed
        queued = rl.try_acquire()
        self.assertTrue(queued["queued"])
        time.sleep(0.15)
        # 等待已超时；process_queue 应清理并返回 None（无令牌）
        rid = rl.process_queue()
        self.assertIsNone(rid)
        # 超时的请求已被剔除
        self.assertEqual(rl.queue.pending_count(), 0)

    def test_stats_shape(self):
        rl = RateLimiter(rate=5.0, capacity=3, queue_size=4, queue_timeout=2.0)
        rl.try_acquire()
        stats = rl.stats()
        for k in ("rate", "capacity", "tokens_available", "queue_size",
                  "queue_timeout", "pending", "allowed", "queued", "rejected"):
            self.assertIn(k, stats)
        self.assertEqual(stats["capacity"], 3)
        self.assertEqual(stats["queue_size"], 4)
        self.assertGreaterEqual(stats["allowed"], 1)


# ── Server 集成（仅限流响应行为） ──────────────────────────────────

class TestServerRateLimitIntegration(unittest.TestCase):
    """通过 HTTPServer 验证 429 / 202 / 200 三种响应路径。"""

    def _start_server(self, rate_limiter):
        from http.server import HTTPServer
        from atomcode_open_api.server import ProxyHandler

        class _Stats:
            total_requests = 0
            total_tokens = 0
            errors = 0
            def record(self, tokens=0, error=False): pass

        class _H(ProxyHandler):
            pass

        _H.gateway = None
        _H.model_mapper = None
        _H.stats = _Stats()
        _H.auth = None
        _H.log_writer = None
        _H.log_db_path = ""
        _H.rate_limiter = rate_limiter
        _H.verbose = False
        _H.log_message = lambda *a, **kw: None

        server = HTTPServer(("127.0.0.1", 0), _H)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, port, thread

    def _post(self, port, body=b"not-json"):
        from http.client import HTTPConnection
        conn = HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request("POST", "/v1/chat/completions", body=body,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        out = (resp.status, resp.read().decode("utf-8"))
        conn.close()
        return out

    def test_429_when_queue_full(self):
        rl = RateLimiter(rate=0.0, capacity=1, queue_size=1)
        server, port, thread = self._start_server(rl)
        try:
            import json
            # 第 1 次：allowed -> 继续后续处理；body 故意发非法 JSON，
            # 让处理器返回 400，避免触达 gateway/model_mapper（用例只关心限流分支）
            s1, _ = self._post(port)
            self.assertEqual(s1, 400)
            # 第 2 次：queued -> 202
            s2, b2 = self._post(port)
            self.assertEqual(s2, 202)
            self.assertEqual(json.loads(b2)["status"], "queued")
            # 第 3 次：rejected -> 429
            s3, b3 = self._post(port)
            self.assertEqual(s3, 429)
            self.assertIn("rate_limit_error", b3)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


if __name__ == "__main__":
    unittest.main()
