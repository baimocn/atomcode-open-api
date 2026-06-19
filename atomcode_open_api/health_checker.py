"""
health_checker - 多网关健康检查与故障转移

提供：
  - SlidingWindowStats：滑动窗口内的成功率与平均延迟统计
  - GatewayHealthChecker：周期性探测网关 /v1/models 端点
  - GatewayPool：管理多个网关并选择最优可用网关
"""

from __future__ import annotations

import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Tuple

from .gateway import Gateway


# 与 gateway.py 行为一致：跳过 SSL 校验
_HC_SSL_CTX = ssl.create_default_context()
_HC_SSL_CTX.check_hostname = False
_HC_SSL_CTX.verify_mode = ssl.CERT_NONE


# ── SlidingWindowStats ──────────────────────────────────────────────

class SlidingWindowStats:
    """
    线程安全的固定窗口大小的探测统计。
    """

    def __init__(self, window_size: int = 60):
        if window_size <= 0:
            raise ValueError("window_size must be > 0")
        self.window_size = int(window_size)
        self._events: Deque[Tuple[float, bool]] = deque(maxlen=self.window_size)
        self._lock = threading.Lock()

    def record(self, latency_ms: float, success: bool) -> None:
        with self._lock:
            self._events.append((float(latency_ms), bool(success)))

    def avg_latency(self) -> Optional[float]:
        """窗口内 *成功* 探测的平均延迟（毫秒）。"""
        with self._lock:
            successes = [lat for lat, ok in self._events if ok and lat >= 0]
            if not successes:
                return None
            return sum(successes) / len(successes)

    def success_rate(self) -> float:
        with self._lock:
            if not self._events:
                return 0.0
            ok = sum(1 for _, s in self._events if s)
            return ok / len(self._events)

    def sample_count(self) -> int:
        with self._lock:
            return len(self._events)

    def is_healthy(self, threshold: float = 0.5,
                   max_latency_ms: float = 5000.0) -> bool:
        """
        成功率 >= threshold 且平均延迟 <= max_latency_ms 视为健康。

        无样本时视为不健康（保守策略）。无成功样本但有失败样本时，
        平均延迟为 None：直接按成功率判定（此时 success_rate=0 -> 不健康）。
        """
        if self.sample_count() == 0:
            return False
        if self.success_rate() < threshold:
            return False
        avg = self.avg_latency()
        if avg is None:
            return False
        return avg <= max_latency_ms


# ── GatewayHealthChecker ────────────────────────────────────────────

class GatewayHealthChecker:
    """
    同步探测 + 后台周期调度。

    - check(): 同步执行一次探测。
    - start(callback): 启动后台守护线程，按 interval 周期调用 callback。
    - stop(): 停止后台线程。
    """

    def __init__(self, probe_url: str = "/v1/models",
                 interval: float = 30.0, timeout: float = 10.0):
        if interval <= 0:
            raise ValueError("interval must be > 0")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        self.probe_url = probe_url
        self.interval = float(interval)
        self.timeout = float(timeout)

        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ── 探测 ──────────────────────────────────────────────────

    def check(self, gateway_url: str, access_token: str) -> dict:
        """
        同步发起一次探测请求，返回结构化结果。
        """
        full_url = gateway_url.rstrip("/") + self.probe_url
        headers = {"Authorization": f"Bearer {access_token}"}
        start = time.monotonic()
        status_code = 0
        error: Optional[str] = None
        try:
            req = urllib.request.Request(full_url, headers=headers, method="GET")
            with urllib.request.urlopen(req, context=_HC_SSL_CTX,
                                        timeout=self.timeout) as resp:
                status_code = resp.status
                # 把响应读掉以释放连接
                try:
                    resp.read()
                except Exception:
                    pass
        except urllib.error.HTTPError as e:
            status_code = e.code
            error = f"HTTP {e.code}"
        except Exception as e:
            status_code = 0
            error = str(e)[:200] or e.__class__.__name__

        latency_ms = int((time.monotonic() - start) * 1000)
        healthy = (200 <= status_code < 500) and error is None
        # 服务端返回 401/403/404 等也算"可达"，故只有 5xx 与网络错误视为不健康
        if 500 <= status_code < 600:
            healthy = False

        return {
            "gateway": gateway_url,
            "healthy": healthy,
            "latency_ms": latency_ms if error is None else -1,
            "status_code": status_code,
            "error": error,
        }

    # ── 后台线程 ─────────────────────────────────────────────

    def start(self, gateway_checker_callback: Callable[[], None]) -> None:
        """
        启动后台守护线程，每 interval 秒调用一次 callback。
        重复调用安全：已运行时直接返回。
        """
        with self._lock:
            if self._worker and self._worker.is_alive():
                return
            self._stop_event.clear()
            self._worker = threading.Thread(
                target=self._run,
                args=(gateway_checker_callback,),
                name="GatewayHealthChecker",
                daemon=True,
            )
            self._worker.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        with self._lock:
            worker = self._worker
        if worker:
            worker.join(timeout=timeout)

    def _run(self, callback: Callable[[], None]) -> None:
        # 启动时立刻执行一次，避免首次等待整个 interval
        try:
            callback()
        except Exception:
            pass
        while not self._stop_event.wait(self.interval):
            try:
                callback()
            except Exception:
                # 周期性回调失败不应让线程退出
                pass


# ── GatewayPool ─────────────────────────────────────────────────────

class GatewayPool:
    """
    多网关池。

    - 内部为每个 URL 维护一个 Gateway 实例 + SlidingWindowStats
    - select() 选择延迟最低的健康网关；全部不健康时降级选成功率最高者
    - report_result() 由调用方在实际请求结束后上报
    - probe_all() 供 GatewayHealthChecker 周期调用，对每个网关做一次探测
    """

    def __init__(self, gateway_urls: List[str], access_token: str,
                 health_checker: GatewayHealthChecker,
                 window_size: int = 60,
                 healthy_threshold: float = 0.5,
                 max_latency_ms: float = 5000.0):
        if not gateway_urls:
            raise ValueError("gateway_urls must be non-empty")

        self.urls: List[str] = [u.rstrip("/") for u in gateway_urls]
        self.access_token = access_token
        self.health_checker = health_checker
        self.healthy_threshold = healthy_threshold
        self.max_latency_ms = max_latency_ms

        self._gateways: Dict[str, Gateway] = {
            url: Gateway(access_token=access_token, gateway_url=url)
            for url in self.urls
        }
        self._stats: Dict[str, SlidingWindowStats] = {
            url: SlidingWindowStats(window_size=window_size) for url in self.urls
        }
        # 手动 down 标记（mark_down）的截止时间戳；> now 表示仍处于 down
        self._down_until: Dict[str, float] = {url: 0.0 for url in self.urls}
        self._lock = threading.Lock()
        # 最近一次 select 返回的 URL（仅供观察）
        self._current: Optional[str] = self.urls[0]

    # ── 网关管理 ──────────────────────────────────────────────

    def get_gateway(self, url: str) -> Gateway:
        gw = self._gateways.get(url.rstrip("/"))
        if gw is None:
            raise KeyError(f"gateway not in pool: {url}")
        return gw

    def report_result(self, gateway_url: str, latency_ms: float,
                       success: bool) -> None:
        url = gateway_url.rstrip("/")
        stats = self._stats.get(url)
        if stats is None:
            return
        stats.record(latency_ms, success)

    def mark_down(self, gateway_url: str, cooldown: float = 60.0) -> None:
        """
        将一个网关在 `cooldown` 秒内标记为不可用（强制不被 select）。
        """
        url = gateway_url.rstrip("/")
        if url not in self._stats:
            return
        with self._lock:
            self._down_until[url] = time.monotonic() + max(0.0, cooldown)

    # ── 选择 ──────────────────────────────────────────────────

    def _is_manually_down(self, url: str) -> bool:
        return self._down_until.get(url, 0.0) > time.monotonic()

    def select(self) -> str:
        """
        选择策略：
          1) 在未被 mark_down 且 is_healthy 的网关中，选 avg_latency 最低的；
             无样本时 avg=+inf 排在最后。
          2) 若没有健康网关：在未被 mark_down 的网关中选 success_rate 最高的（降级）。
          3) 若全部被 mark_down：忽略 down 标记，选 success_rate 最高的。
        """
        if not self.urls:
            raise RuntimeError("gateway pool is empty")

        # 1) 健康集
        healthy: List[Tuple[float, str]] = []
        for url in self.urls:
            if self._is_manually_down(url):
                continue
            stats = self._stats[url]
            if stats.is_healthy(self.healthy_threshold, self.max_latency_ms):
                avg = stats.avg_latency()
                healthy.append((avg if avg is not None else float("inf"), url))

        if healthy:
            healthy.sort()
            chosen = healthy[0][1]
            with self._lock:
                self._current = chosen
            return chosen

        # 2) 降级：未 mark_down 的中选 success_rate 最高者；并列时取样本最多者
        def _score(url: str) -> Tuple[float, int]:
            s = self._stats[url]
            return (s.success_rate(), s.sample_count())

        candidates = [u for u in self.urls if not self._is_manually_down(u)]
        if not candidates:
            # 3) 全部被 mark_down：兜底
            candidates = list(self.urls)

        candidates.sort(key=_score, reverse=True)
        chosen = candidates[0]
        with self._lock:
            self._current = chosen
        return chosen

    # ── 健康探测调度 ─────────────────────────────────────────

    def probe_all(self) -> List[dict]:
        """
        对池内所有网关各做一次同步探测，并把结果写入 stats。
        返回每个网关的探测结果列表。
        """
        results: List[dict] = []
        for url in self.urls:
            res = self.health_checker.check(url, self.access_token)
            self.report_result(url, res["latency_ms"], res["healthy"])
            results.append(res)
        return results

    # ── 状态 ──────────────────────────────────────────────────

    def status(self) -> dict:
        now = time.monotonic()
        gateways = []
        for url in self.urls:
            s = self._stats[url]
            gateways.append({
                "url": url,
                "healthy": (
                    not self._is_manually_down(url)
                    and s.is_healthy(self.healthy_threshold, self.max_latency_ms)
                ),
                "success_rate": round(s.success_rate(), 4),
                "avg_latency_ms": (
                    round(s.avg_latency(), 2) if s.avg_latency() is not None else None
                ),
                "samples": s.sample_count(),
                "down_until_in": (
                    max(0.0, round(self._down_until[url] - now, 2))
                    if self._is_manually_down(url) else 0.0
                ),
            })
        return {
            "current": self._current,
            "gateways": gateways,
        }
