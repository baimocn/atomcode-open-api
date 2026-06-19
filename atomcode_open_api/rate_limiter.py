"""
rate_limiter - 令牌桶速率限制 + 请求优先级队列

设计目标：
  - 用于在 AtomCode 共享代理服务上控制对上游网关的调用速率，避免单一
    使用方瞬时打爆配额。
  - 当桶中暂时无可用令牌时，请求被排入优先级队列，等待后续被处理；
    队列已满则拒绝并提示用户限流。
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple


# ── TokenBucket ─────────────────────────────────────────────────────

class TokenBucket:
    """
    线程安全的令牌桶。

    - rate: 每秒补充的令牌数（float）
    - capacity: 桶最大容量
    - 惰性补充：仅在 acquire 时根据时间差补充令牌，不使用后台线程
    """

    def __init__(self, rate: float, capacity: int):
        if rate < 0:
            raise ValueError("rate must be >= 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.rate = float(rate)
        self.capacity = int(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    # ── 内部 ──────────────────────────────────────────────────

    def _refill_locked(self) -> None:
        """在已持锁的情况下，根据时间差补充令牌。"""
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

    # ── 公共 API ──────────────────────────────────────────────

    def acquire(self, tokens: int = 1, timeout: float = 0.0) -> bool:
        """
        尝试获取 `tokens` 个令牌。

        - timeout <= 0：立即返回，成功 True / 失败 False
        - timeout > 0 ：最多等待 timeout 秒，期间会周期性休眠重试
        """
        if tokens <= 0:
            return True
        if tokens > self.capacity:
            # 永远无法满足
            return False

        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                missing = tokens - self._tokens
                # 估算等待至有足够令牌所需时间
                wait = (missing / self.rate) if self.rate > 0 else float("inf")

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            # 短睡眠避免空转；最多睡到 deadline
            time.sleep(min(remaining, max(0.005, wait)))

    def token_count(self) -> float:
        """返回当前可用令牌数（含此刻惰性补充后的值）。"""
        with self._lock:
            self._refill_locked()
            return self._tokens


# ── RequestQueue ────────────────────────────────────────────────────

class RequestQueue:
    """
    基于 heapq 的优先级请求队列。

    堆条目: (priority, seq, enqueue_ts, request_id)
      - priority 越小优先级越高
      - seq 用于在相同优先级下保证 FIFO
      - enqueue_ts 是入队时刻（time.monotonic），用于 is_expired 判断
    """

    def __init__(self, max_size: int, timeout: float = 30.0):
        if max_size <= 0:
            raise ValueError("max_size must be > 0")
        self.max_size = int(max_size)
        self.timeout = float(timeout)

        self._heap: List[Tuple[int, int, float, str]] = []
        self._index: Dict[str, Tuple[int, int, float]] = {}  # id -> (priority, seq, ts)
        self._counter = itertools.count()
        self._lock = threading.Lock()

    # ── 公共 API ──────────────────────────────────────────────

    def enqueue(self, request_id: str, priority: int = 0) -> bool:
        """入队；队列满返回 False。同一 id 已存在视为入队失败。"""
        with self._lock:
            if len(self._heap) >= self.max_size:
                return False
            if request_id in self._index:
                return False
            seq = next(self._counter)
            ts = time.monotonic()
            entry = (priority, seq, ts, request_id)
            heapq.heappush(self._heap, entry)
            self._index[request_id] = (priority, seq, ts)
            return True

    def dequeue(self) -> Optional[str]:
        """取出优先级最高的请求 id，空返回 None。"""
        with self._lock:
            while self._heap:
                priority, seq, ts, rid = heapq.heappop(self._heap)
                # 校验仍然有效（未被外部移除）
                idx = self._index.pop(rid, None)
                if idx is not None and idx == (priority, seq, ts):
                    return rid
            return None

    def pending_count(self) -> int:
        with self._lock:
            return len(self._heap)

    def is_expired(self, request_id: str) -> bool:
        """检查指定请求在队列中等待是否已超过 timeout。"""
        with self._lock:
            idx = self._index.get(request_id)
            if idx is None:
                # 不在队列中：视为非排队状态，不算「正在等待中的超时」
                return False
            _, _, ts = idx
            return (time.monotonic() - ts) > self.timeout

    def remove_expired(self) -> List[str]:
        """
        清理所有已超时的条目，返回被移除的 request_id 列表。
        """
        removed: List[str] = []
        now = time.monotonic()
        with self._lock:
            kept: List[Tuple[int, int, float, str]] = []
            for priority, seq, ts, rid in self._heap:
                if (now - ts) > self.timeout:
                    removed.append(rid)
                    self._index.pop(rid, None)
                else:
                    kept.append((priority, seq, ts, rid))
            if removed:
                heapq.heapify(kept)
                self._heap = kept
        return removed

    def position(self, request_id: str) -> int:
        """
        返回请求在当前优先级排序中的 1-based 位置；不在队列返回 0。
        位置仅在调用时刻有效，仅供提示。
        """
        with self._lock:
            target = self._index.get(request_id)
            if target is None:
                return 0
            target_key = (target[0], target[1])
            rank = 1
            for priority, seq, _, rid in self._heap:
                if rid == request_id:
                    continue
                if (priority, seq) < target_key:
                    rank += 1
            return rank


# ── RateLimiter ─────────────────────────────────────────────────────

class RateLimiter:
    """整合 TokenBucket + RequestQueue 的限流外观。"""

    def __init__(self, rate: float, capacity: int, queue_size: int,
                 queue_timeout: float = 30.0):
        self.bucket = TokenBucket(rate=rate, capacity=capacity)
        self.queue = RequestQueue(max_size=queue_size, timeout=queue_timeout)
        # 计数（仅供观察）
        self._allowed = 0
        self._queued = 0
        self._rejected = 0
        self._stats_lock = threading.Lock()

    # ── 公共 API ──────────────────────────────────────────────

    def try_acquire(self, priority: int = 0) -> dict:
        """
        尝试获取一次调用名额。

        Returns:
          {
            "allowed": bool,
            "queued": bool,
            "queue_position": int,    # 入队时的 1-based 位置；未入队为 0
            "request_id": str | None, # 入队成功时返回，便于后续追踪
            "message": str
          }
        """
        # 1) 立即尝试拿令牌
        if self.bucket.acquire(1, timeout=0):
            with self._stats_lock:
                self._allowed += 1
            return {
                "allowed": True,
                "queued": False,
                "queue_position": 0,
                "request_id": None,
                "message": "ok",
            }

        # 2) 入队等待
        request_id = uuid.uuid4().hex
        if self.queue.enqueue(request_id, priority=priority):
            position = self.queue.position(request_id)
            with self._stats_lock:
                self._queued += 1
            return {
                "allowed": False,
                "queued": True,
                "queue_position": position,
                "request_id": request_id,
                "message": "rate limited, request queued",
            }

        # 3) 队列也满了
        with self._stats_lock:
            self._rejected += 1
        return {
            "allowed": False,
            "queued": False,
            "queue_position": 0,
            "request_id": None,
            "message": "rate limited and queue is full",
        }

    def process_queue(self) -> Optional[str]:
        """
        处理队列：先剔除所有已超时的请求；如果当前有令牌可用，弹出下
        一个高优先级请求并消费一个令牌，返回其 request_id；否则返回
        None。
        """
        self.queue.remove_expired()
        if self.queue.pending_count() == 0:
            return None
        if not self.bucket.acquire(1, timeout=0):
            return None
        rid = self.queue.dequeue()
        if rid is None:
            # 令牌已扣但队列突然为空，归还令牌（极小概率竞争）
            with self.bucket._lock:  # type: ignore[attr-defined]
                self.bucket._tokens = min(  # type: ignore[attr-defined]
                    self.bucket.capacity, self.bucket._tokens + 1  # type: ignore[attr-defined]
                )
            return None
        with self._stats_lock:
            self._allowed += 1
        return rid

    def stats(self) -> dict:
        with self._stats_lock:
            return {
                "rate": self.bucket.rate,
                "capacity": self.bucket.capacity,
                "tokens_available": round(self.bucket.token_count(), 3),
                "queue_size": self.queue.max_size,
                "queue_timeout": self.queue.timeout,
                "pending": self.queue.pending_count(),
                "allowed": self._allowed,
                "queued": self._queued,
                "rejected": self._rejected,
            }
