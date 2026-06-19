"""
logger_db - 请求日志持久化与异步批量写入

使用 SQLite (WAL 模式) 存储请求日志，并通过独立后台线程进行批量
flush，避免阻塞主请求线程。
"""

import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


# ── 数据库结构 ─────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS request_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    model_requested TEXT,
    model_mapped TEXT,
    status_code INTEGER,
    latency_ms INTEGER,
    is_stream INTEGER,
    token_count_prompt INTEGER,
    token_count_completion INTEGER,
    error_message TEXT,
    gateway_used TEXT
)
"""

CREATE_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_request_logs_ts ON request_logs(timestamp)"
)

INSERT_SQL = """
INSERT INTO request_logs (
    timestamp, method, path, model_requested, model_mapped,
    status_code, latency_ms, is_stream, token_count_prompt,
    token_count_completion, error_message, gateway_used
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass
class LogEntry:
    """单条请求日志"""
    timestamp: int
    method: str
    path: str
    model_requested: Optional[str] = None
    model_mapped: Optional[str] = None
    status_code: Optional[int] = None
    latency_ms: Optional[int] = None
    is_stream: bool = False
    token_count_prompt: Optional[int] = None
    token_count_completion: Optional[int] = None
    error_message: Optional[str] = None
    gateway_used: Optional[str] = None

    def to_row(self) -> tuple:
        return (
            self.timestamp,
            self.method,
            self.path,
            self.model_requested,
            self.model_mapped,
            self.status_code,
            self.latency_ms,
            1 if self.is_stream else 0,
            self.token_count_prompt,
            self.token_count_completion,
            self.error_message,
            self.gateway_used,
        )


def init_db(db_path: str) -> None:
    """
    初始化数据库：建表 + 启用 WAL。

    Args:
        db_path: SQLite 数据库文件路径
    """
    parent = os.path.dirname(os.path.abspath(db_path))
    if parent:
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(CREATE_TABLE_SQL)
        conn.execute(CREATE_INDEX_SQL)
        conn.commit()
    finally:
        conn.close()


# ── AsyncLogWriter ────────────────────────────────────────────────

class AsyncLogWriter:
    """
    异步批量日志写入器。

    工作模式：
      - 主线程通过 log() 非阻塞地把 LogEntry 投入内部 queue
      - 后台线程定期消费 queue，按 batch_size 或 flush_interval 进行 flush
      - flush 时使用一次事务批量 INSERT
    """

    DEFAULT_BATCH_SIZE = 50
    DEFAULT_FLUSH_INTERVAL = 5.0  # 秒

    def __init__(self, db_path: str,
                 batch_size: int = DEFAULT_BATCH_SIZE,
                 flush_interval: float = DEFAULT_FLUSH_INTERVAL,
                 auto_start: bool = True):
        self.db_path = db_path
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self._queue: "queue.Queue[LogEntry]" = queue.Queue()
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # 共享 pending（worker 从 queue 取出但尚未写盘的项），加锁访问
        self._pending: List[LogEntry] = []
        self._pending_lock = threading.Lock()

        # 计数器（便于测试观察）
        self._total_written = 0
        self._write_lock = threading.Lock()

        init_db(self.db_path)

        if auto_start:
            self.start()

    # ── lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._worker and self._worker.is_alive():
                return
            self._stop_event.clear()
            self._worker = threading.Thread(
                target=self._run, name="AsyncLogWriter", daemon=True
            )
            self._worker.start()

    def stop(self, drain: bool = True, timeout: float = 10.0) -> None:
        """停止后台线程；drain=True 时先把队列剩余项 flush 完。"""
        self._stop_event.set()
        if self._worker:
            self._worker.join(timeout=timeout)
        if drain:
            # 把可能残留的项再 flush 一次（同步调用，确保落盘）
            self.flush_now()

    # ── 公共 API ──────────────────────────────────────────────

    def log(self, entry: LogEntry) -> None:
        """非阻塞投递一条日志。"""
        self._queue.put(entry)

    def log_dict(self, **kwargs: Any) -> None:
        """便捷写法：传字段 kwargs。"""
        self.log(LogEntry(**kwargs))

    def flush_now(self) -> int:
        """
        同步把当前队列 + 后台 pending 中的所有项立刻写入数据库。
        返回这次 flush 写入的条数。
        """
        items: List[LogEntry] = []
        with self._pending_lock:
            if self._pending:
                items.extend(self._pending)
                self._pending.clear()
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if not items:
            return 0
        self._write_batch(items)
        return len(items)

    @property
    def total_written(self) -> int:
        with self._write_lock:
            return self._total_written

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    # ── 内部 ──────────────────────────────────────────────────

    def _run(self) -> None:
        """后台线程主循环"""
        last_flush = time.time()

        while not self._stop_event.is_set():
            timeout = max(0.05, self.flush_interval - (time.time() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
                with self._pending_lock:
                    self._pending.append(item)
            except queue.Empty:
                pass

            now = time.time()
            with self._pending_lock:
                size = len(self._pending)
                should_flush = (
                    size >= self.batch_size
                    or (size > 0 and (now - last_flush) >= self.flush_interval)
                )
                if should_flush:
                    batch = self._pending
                    self._pending = []
                else:
                    batch = None
            if batch:
                self._write_batch(batch)
                last_flush = now

        # 退出前把剩余 pending 写入
        with self._pending_lock:
            tail = self._pending
            self._pending = []
        if tail:
            self._write_batch(tail)

    def _write_batch(self, items: List[LogEntry]) -> None:
        if not items:
            return
        rows = [it.to_row() for it in items]
        # SQLite 连接不可跨线程共享，每次创建新连接
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            with conn:
                conn.executemany(INSERT_SQL, rows)
        finally:
            conn.close()
        with self._write_lock:
            self._total_written += len(rows)


# ── 统计查询 ──────────────────────────────────────────────────────

def query_stats(db_path: str, hours: int = 24) -> Dict[str, Any]:
    """
    查询最近 `hours` 小时的统计摘要。

    Returns:
        dict 形式的统计信息，字段：
          - total_requests
          - success_rate
          - avg_latency_ms
          - p95_latency_ms
          - top_models  (list of {model, count})
          - error_breakdown  (dict of status_code -> count)
          - window_hours
          - since (unix ts)
    """
    if hours <= 0:
        hours = 24

    since_ts = int(time.time()) - hours * 3600

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # 总请求数 + 平均延迟
        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   AVG(latency_ms) AS avg_latency,
                   SUM(CASE WHEN status_code >= 200 AND status_code < 300
                            THEN 1 ELSE 0 END) AS success_count
            FROM request_logs
            WHERE timestamp >= ?
            """,
            (since_ts,),
        )
        row = cur.fetchone()
        total = row["total"] or 0
        avg_latency = float(row["avg_latency"]) if row["avg_latency"] is not None else 0.0
        success_count = row["success_count"] or 0
        success_rate = (success_count / total) if total > 0 else 0.0

        # p95 延迟（用纯 SQL 近似：取排序后的第 ceil(0.95 * N) 项）
        p95 = 0.0
        if total > 0:
            cur.execute(
                """
                SELECT latency_ms FROM request_logs
                WHERE timestamp >= ? AND latency_ms IS NOT NULL
                ORDER BY latency_ms ASC
                """,
                (since_ts,),
            )
            latencies = [r["latency_ms"] for r in cur.fetchall()]
            if latencies:
                # p95 索引：ceil(0.95*N) - 1，限制在 [0, N-1]
                import math
                idx = max(0, min(len(latencies) - 1, math.ceil(0.95 * len(latencies)) - 1))
                p95 = float(latencies[idx])

        # top 5 模型
        cur.execute(
            """
            SELECT COALESCE(model_mapped, model_requested, '') AS model,
                   COUNT(*) AS cnt
            FROM request_logs
            WHERE timestamp >= ?
            GROUP BY model
            ORDER BY cnt DESC
            LIMIT 5
            """,
            (since_ts,),
        )
        top_models = [{"model": r["model"], "count": r["cnt"]} for r in cur.fetchall()]

        # 错误码分布（非 2xx）
        cur.execute(
            """
            SELECT status_code, COUNT(*) AS cnt
            FROM request_logs
            WHERE timestamp >= ?
              AND (status_code < 200 OR status_code >= 300)
            GROUP BY status_code
            ORDER BY cnt DESC
            """,
            (since_ts,),
        )
        error_breakdown = {
            str(r["status_code"] if r["status_code"] is not None else "null"): r["cnt"]
            for r in cur.fetchall()
        }

        return {
            "window_hours": hours,
            "since": since_ts,
            "total_requests": total,
            "success_rate": round(success_rate, 4),
            "avg_latency_ms": round(avg_latency, 2),
            "p95_latency_ms": round(p95, 2),
            "top_models": top_models,
            "error_breakdown": error_breakdown,
        }
    finally:
        conn.close()


# ── 默认数据库路径 ─────────────────────────────────────────────────

def default_db_path() -> str:
    """默认日志数据库路径：~/.atomcode_open_api/request_logs.db"""
    home = os.path.expanduser("~")
    return os.path.join(home, ".atomcode_open_api", "request_logs.db")
