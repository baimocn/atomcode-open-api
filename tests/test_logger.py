"""
test_logger - logger_db & /stats 端点的单元测试

覆盖：
  1) AsyncLogWriter 的基本写入与显式 flush
  2) /stats 端点返回的 JSON 结构与数值
  3) 批量写入：60 条写入后数据库中至少 50 条已 flush
  4) 多线程并发写入安全性（3 个线程同时写入）
"""

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import HTTPServer

# 让 tests 目录可以独立运行：把项目根目录加入 sys.path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from atomcode_open_api.logger_db import (  # noqa: E402
    AsyncLogWriter,
    LogEntry,
    init_db,
    query_stats,
)


def _count_rows(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM request_logs")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _make_entry(status: int = 200, latency: int = 100,
                model: str = "gpt-4", is_stream: bool = False,
                ts: int = None) -> LogEntry:
    return LogEntry(
        timestamp=int(ts if ts is not None else time.time()),
        method="POST",
        path="/v1/chat/completions",
        model_requested=model,
        model_mapped=model,
        status_code=status,
        latency_ms=latency,
        is_stream=is_stream,
        token_count_prompt=10 if not is_stream else -1,
        token_count_completion=20 if not is_stream else -1,
        error_message=None if 200 <= status < 300 else "boom",
        gateway_used="https://test.example.com",
    )


class TestInitDB(unittest.TestCase):
    def test_creates_table_and_wal(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "x.db")
            init_db(db)
            self.assertTrue(os.path.exists(db))

            conn = sqlite3.connect(db)
            try:
                # 检查表存在
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='request_logs'"
                )
                self.assertIsNotNone(cur.fetchone())
                # 检查 WAL 模式
                cur = conn.execute("PRAGMA journal_mode")
                mode = cur.fetchone()[0]
                self.assertEqual(mode.lower(), "wal")
            finally:
                conn.close()


class TestAsyncLogWriterBasic(unittest.TestCase):
    def test_log_and_flush_now(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, batch_size=50, flush_interval=5.0,
                                    auto_start=False)
            try:
                for i in range(10):
                    writer.log(_make_entry(latency=i * 10))
                written = writer.flush_now()
                self.assertEqual(written, 10)
                self.assertEqual(_count_rows(db), 10)
            finally:
                writer.stop(drain=True, timeout=2.0)

    def test_background_flush_interval(self):
        """flush_interval 触发：少量条目，等到时间到时应被 flush。"""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, batch_size=50, flush_interval=0.3)
            try:
                for _ in range(5):
                    writer.log(_make_entry())
                # 等待 > flush_interval
                deadline = time.time() + 3.0
                while time.time() < deadline and _count_rows(db) < 5:
                    time.sleep(0.05)
                self.assertEqual(_count_rows(db), 5)
            finally:
                writer.stop(drain=True, timeout=2.0)


class TestBatchFlush(unittest.TestCase):
    def test_60_writes_at_least_50_flushed(self):
        """写 60 条，batch_size=50，预期数据库中至少 50 条已 flush。"""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, batch_size=50, flush_interval=10.0)
            try:
                for i in range(60):
                    writer.log(_make_entry(latency=i))

                # 等待 batch-size 触发的 flush 发生
                deadline = time.time() + 3.0
                while time.time() < deadline:
                    if _count_rows(db) >= 50:
                        break
                    time.sleep(0.02)

                count = _count_rows(db)
                self.assertGreaterEqual(
                    count, 50,
                    f"expected >=50 rows after batch flush, got {count}"
                )

                # 把剩余的也 flush 掉验证总数
                writer.flush_now()
                self.assertEqual(_count_rows(db), 60)
            finally:
                writer.stop(drain=True, timeout=2.0)


class TestConcurrentWrites(unittest.TestCase):
    def test_three_threads_concurrent(self):
        """3 个线程同时写入，验证无丢失、无异常。"""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, batch_size=20, flush_interval=0.5)
            per_thread = 100
            errors: list = []

            def worker(idx: int):
                try:
                    for i in range(per_thread):
                        writer.log(_make_entry(
                            latency=idx * 1000 + i,
                            model=f"model-t{idx}",
                        ))
                except Exception as e:  # pragma: no cover
                    errors.append(e)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
            try:
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                # 等待异步 flush 完成
                deadline = time.time() + 5.0
                expected = 3 * per_thread
                while time.time() < deadline:
                    if _count_rows(db) >= expected:
                        break
                    time.sleep(0.05)
                writer.flush_now()

                self.assertEqual(errors, [])
                self.assertEqual(_count_rows(db), expected)
            finally:
                writer.stop(drain=True, timeout=2.0)


class TestQueryStats(unittest.TestCase):
    def test_stats_shape_and_values(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, batch_size=10, flush_interval=0.1,
                                    auto_start=False)
            try:
                now = int(time.time())
                # 8 条 200 + 2 条 500
                latencies_ok = [10, 20, 30, 40, 50, 60, 70, 80]
                for lat in latencies_ok:
                    writer.log(_make_entry(status=200, latency=lat, model="m-a", ts=now))
                writer.log(_make_entry(status=500, latency=900, model="m-b", ts=now))
                writer.log(_make_entry(status=404, latency=15, model="m-a", ts=now))
                writer.flush_now()

                stats = query_stats(db, hours=24)

                # 结构字段都在
                for k in ("total_requests", "success_rate", "avg_latency_ms",
                          "p95_latency_ms", "top_models", "error_breakdown",
                          "window_hours", "since"):
                    self.assertIn(k, stats)

                self.assertEqual(stats["total_requests"], 10)
                self.assertAlmostEqual(stats["success_rate"], 0.8, places=4)

                # avg = (10+20+...+80 + 900 + 15)/10 = (360 + 915)/10 = 127.5
                self.assertAlmostEqual(stats["avg_latency_ms"], 127.5, places=2)

                # top_models 按 count 降序
                self.assertEqual(stats["top_models"][0]["model"], "m-a")
                self.assertEqual(stats["top_models"][0]["count"], 9)

                # 错误分布包含 500 与 404
                self.assertIn("500", stats["error_breakdown"])
                self.assertIn("404", stats["error_breakdown"])
                self.assertEqual(stats["error_breakdown"]["500"], 1)
                self.assertEqual(stats["error_breakdown"]["404"], 1)

                # p95 必须 > 80（因为 900 在尾部）
                self.assertGreaterEqual(stats["p95_latency_ms"], 80.0)
            finally:
                writer.stop(drain=True, timeout=2.0)

    def test_hours_filter(self):
        """超出时间窗口的记录不应被统计。"""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, auto_start=False)
            try:
                now = int(time.time())
                # 旧的（48 小时前）
                writer.log(_make_entry(status=200, ts=now - 48 * 3600))
                # 新的
                writer.log(_make_entry(status=200, ts=now))
                writer.log(_make_entry(status=200, ts=now))
                writer.flush_now()

                stats_24 = query_stats(db, hours=24)
                self.assertEqual(stats_24["total_requests"], 2)

                stats_72 = query_stats(db, hours=72)
                self.assertEqual(stats_72["total_requests"], 3)
            finally:
                writer.stop(drain=True, timeout=2.0)


class TestStatsEndpoint(unittest.TestCase):
    """端到端测试 /stats HTTP 端点。"""

    def _start_server(self, db_path: str, writer: AsyncLogWriter):
        from atomcode_open_api.server import ProxyHandler

        class _H(ProxyHandler):
            pass

        _H.gateway = None
        _H.model_mapper = None
        _H.stats = None
        _H.auth = None
        _H.log_writer = writer
        _H.log_db_path = db_path
        _H.verbose = False

        # 静默 BaseHTTPRequestHandler 的日志输出
        _H.log_message = lambda *a, **kw: None

        server = HTTPServer(("127.0.0.1", 0), _H)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, port, thread

    def test_stats_endpoint_json(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, auto_start=False)
            now = int(time.time())
            writer.log(_make_entry(status=200, latency=50, ts=now))
            writer.log(_make_entry(status=200, latency=70, ts=now))
            writer.log(_make_entry(status=500, latency=200, ts=now))
            writer.flush_now()

            server, port, thread = self._start_server(db, writer)
            try:
                conn = HTTPConnection("127.0.0.1", port, timeout=5)
                conn.request("GET", "/stats?hours=1")
                resp = conn.getresponse()
                self.assertEqual(resp.status, 200)
                body = resp.read().decode("utf-8")
                data = json.loads(body)

                self.assertEqual(data["total_requests"], 3)
                self.assertAlmostEqual(data["success_rate"], 2 / 3, places=2)
                self.assertIn("p95_latency_ms", data)
                self.assertIn("top_models", data)
                self.assertIn("error_breakdown", data)
                self.assertEqual(data["window_hours"], 1)
                conn.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2.0)
                writer.stop(drain=True, timeout=2.0)

    def test_stats_endpoint_invalid_hours_falls_back_to_default(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "log.db")
            writer = AsyncLogWriter(db, auto_start=False)
            writer.flush_now()

            server, port, thread = self._start_server(db, writer)
            try:
                conn = HTTPConnection("127.0.0.1", port, timeout=5)
                conn.request("GET", "/stats?hours=abc")
                resp = conn.getresponse()
                self.assertEqual(resp.status, 200)
                data = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(data["window_hours"], 24)
                conn.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2.0)
                writer.stop(drain=True, timeout=2.0)


if __name__ == "__main__":
    unittest.main()
