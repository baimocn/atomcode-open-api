"""
server - OpenAI 兼容的 HTTP 代理服务器

提供标准的 OpenAI API 端点（/v1/chat/completions, /v1/models），
将请求转发到 AtomCode API 网关，自动处理签名和模型名称映射。
"""

import http.server
import json
import socketserver
import sys
import threading
import time
from typing import Optional, Tuple

from . import __version__
from .auth import AuthInfo
from .gateway import Gateway, GatewayError
from .health_checker import GatewayPool
from .logger_db import AsyncLogWriter, LogEntry, default_db_path, query_stats
from .models import ModelMapper
from .rate_limiter import RateLimiter


class RequestStats:
    """请求统计"""

    def __init__(self):
        self.total_requests = 0
        self.total_tokens = 0
        self.errors = 0
        self.start_time = time.time()
        self._lock = threading.Lock()

    def record(self, tokens: int = 0, error: bool = False):
        with self._lock:
            self.total_requests += 1
            self.total_tokens += tokens
            if error:
                self.errors += 1

    @property
    def uptime_display(self) -> str:
        secs = int(time.time() - self.start_time)
        if secs < 60:
            return f"{secs}s"
        if secs < 3600:
            return f"{secs // 60}m {secs % 60}s"
        hours = secs // 3600
        return f"{hours}h {secs % 3600 // 60}m"


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    """OpenAI 兼容的代理请求处理器"""

    # 这些属性在 create_handler_class 中注入
    gateway: Gateway = None  # type: ignore
    model_mapper: ModelMapper = None  # type: ignore
    stats: RequestStats = None  # type: ignore
    auth: AuthInfo = None  # type: ignore
    log_writer: Optional[AsyncLogWriter] = None
    log_db_path: str = ""
    rate_limiter: Optional[RateLimiter] = None
    gateway_pool: Optional[GatewayPool] = None
    verbose: bool = False

    def do_GET(self):
        if self.path == "/v1/models":
            self._handle_models()
        elif self.path == "/health":
            self._handle_health()
        elif self.path == "/":
            self._handle_index()
        elif self.path == "/stats" or self.path.startswith("/stats?"):
            self._handle_stats()
        else:
            self._respond_json(404, {"error": {"message": "Not found", "type": "invalid_request_error"}})

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len > 0 else b""

        if self.path == "/v1/chat/completions":
            self._handle_chat_completions(body)
        else:
            self._respond_json(404, {"error": {"message": "Not found", "type": "invalid_request_error"}})

    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    # ── Handlers ──────────────────────────────────────────────────────

    def _handle_chat_completions(self, body: bytes):
        """处理 /v1/chat/completions 请求"""
        start_ts = time.time()

        # 速率限制（可选）
        if self.rate_limiter is not None:
            decision = self.rate_limiter.try_acquire()
            if not decision["allowed"]:
                if decision["queued"]:
                    self._respond_json(202, {
                        "status": "queued",
                        "queue_position": decision["queue_position"],
                        "request_id": decision.get("request_id"),
                        "message": decision["message"],
                    })
                    self._log_request(
                        start_ts=start_ts,
                        status_code=202,
                        model_requested="",
                        model_mapped="",
                        is_stream=False,
                        error_message="queued: rate limited",
                    )
                else:
                    self._respond_json(429, {
                        "error": {
                            "message": decision["message"],
                            "type": "rate_limit_error",
                        }
                    })
                    self._log_request(
                        start_ts=start_ts,
                        status_code=429,
                        model_requested="",
                        model_mapped="",
                        is_stream=False,
                        error_message=decision["message"],
                    )
                return

        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            self._respond_json(400, {"error": {"message": "Invalid JSON body", "type": "invalid_request_error"}})
            self._log_request(
                start_ts=start_ts,
                status_code=400,
                model_requested="",
                model_mapped="",
                is_stream=False,
                error_message="Invalid JSON body",
            )
            return

        # 映射模型名称
        original_model = req.get("model", "")
        mapped_model = self.model_mapper.resolve(original_model)
        if mapped_model != original_model:
            req["model"] = mapped_model
            if self.verbose:
                self._log(f"Model: {original_model} -> {mapped_model}")

        is_stream = req.get("stream", False)
        body = json.dumps(req).encode()

        if is_stream:
            self._handle_stream(body, start_ts, original_model, mapped_model)
        else:
            self._handle_non_stream(body, start_ts, original_model, mapped_model)

    def _select_gateway(self) -> Tuple[Gateway, str]:
        """根据是否启用 gateway_pool，选择本次请求使用的 Gateway 与 URL。"""
        if self.gateway_pool is not None:
            url = self.gateway_pool.select()
            return self.gateway_pool.get_gateway(url), url
        return self.gateway, getattr(self.gateway, "gateway_url", "")

    def _handle_non_stream(self, body: bytes, start_ts: float,
                            original_model: str, mapped_model: str):
        """处理非流式请求"""
        prompt_tokens: Optional[int] = None
        completion_tokens: Optional[int] = None
        error_message: Optional[str] = None
        gateway, gw_url = self._select_gateway()
        req_start = time.monotonic()
        try:
            status, resp_body = gateway.chat_completions(body)
            if status == 200:
                try:
                    data = json.loads(resp_body)
                    usage = data.get("usage", {}) or {}
                    tokens = usage.get("total_tokens", 0)
                    prompt_tokens = usage.get("prompt_tokens")
                    completion_tokens = usage.get("completion_tokens")
                    self.stats.record(tokens=tokens)
                except Exception:
                    self.stats.record()
            else:
                self.stats.record(error=True)
                try:
                    error_message = resp_body.decode("utf-8", errors="replace")[:500]
                except Exception:
                    error_message = None
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(resp_body)

            self._log_request(
                start_ts=start_ts,
                status_code=status,
                model_requested=original_model,
                model_mapped=mapped_model,
                is_stream=False,
                token_count_prompt=prompt_tokens,
                token_count_completion=completion_tokens,
                error_message=error_message,
            )
            # 上报到 GatewayPool（如启用）
            if self.gateway_pool is not None and gw_url:
                lat = int((time.monotonic() - req_start) * 1000)
                success = 200 <= status < 500
                self.gateway_pool.report_result(gw_url, lat, success)
                if status >= 500:
                    self.gateway_pool.mark_down(gw_url)
        except GatewayError as e:
            self.stats.record(error=True)
            status = e.status or 502
            self._respond_json(status, {"error": {"message": e.message, "type": "server_error"}})
            self._log_request(
                start_ts=start_ts,
                status_code=status,
                model_requested=original_model,
                model_mapped=mapped_model,
                is_stream=False,
                error_message=str(e.message)[:500],
            )
            if self.gateway_pool is not None and gw_url:
                self.gateway_pool.report_result(gw_url, -1, False)
                self.gateway_pool.mark_down(gw_url)

    def _handle_stream(self, body: bytes, start_ts: float,
                       original_model: str, mapped_model: str):
        """处理流式请求"""
        status = 200
        error_message: Optional[str] = None
        gateway, gw_url = self._select_gateway()
        req_start = time.monotonic()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self._send_cors_headers()
            self.end_headers()

            for line in gateway.chat_completions_stream(body):
                self.wfile.write(line + b"\n")
                self.wfile.flush()

            self.stats.record()
        except Exception as e:
            self.stats.record(error=True)
            status = 500
            error_message = str(e)[:500]
            error_data = json.dumps({"error": {"message": str(e)}})
            try:
                self.wfile.write(f"data: {error_data}\n".encode())
                self.wfile.write(b"data: [DONE]\n")
                self.wfile.flush()
            except Exception:
                pass
        finally:
            if self.gateway_pool is not None and gw_url:
                lat = int((time.monotonic() - req_start) * 1000) if status == 200 else -1
                self.gateway_pool.report_result(gw_url, lat, status == 200)
                if status >= 500:
                    self.gateway_pool.mark_down(gw_url)
            self._log_request(
                start_ts=start_ts,
                status_code=status,
                model_requested=original_model,
                model_mapped=mapped_model,
                is_stream=True,
                # 流式响应无法从 SSE 中可靠汇总 token 用量
                token_count_prompt=-1,
                token_count_completion=-1,
                error_message=error_message,
            )

    def _handle_models(self):
        """处理 /v1/models 请求"""
        try:
            models_data = self.gateway.fetch_models()
            self.model_mapper.update_from_api(models_data)
            self._respond_json(200, self.model_mapper.get_openai_models_response())
        except GatewayError as e:
            self._respond_json(e.status or 502, {"error": {"message": e.message}})

    def _handle_stats(self):
        """处理 /stats 请求：返回最近 N 小时的统计摘要"""
        from urllib.parse import urlparse, parse_qs

        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query or "")
        try:
            hours = int(qs.get("hours", ["24"])[0])
        except (TypeError, ValueError):
            hours = 24
        if hours <= 0:
            hours = 24

        if not self.log_db_path:
            self._respond_json(503, {"error": {"message": "logging not enabled"}})
            return

        # 在查询前先把 writer 中的 pending 项写入，确保统计反映最新数据
        if self.log_writer is not None:
            try:
                self.log_writer.flush_now()
            except Exception:
                pass

        try:
            data = query_stats(self.log_db_path, hours=hours)
            self._respond_json(200, data)
        except Exception as e:
            self._respond_json(500, {"error": {"message": f"stats query failed: {e}"}})

    def _handle_health(self):
        """处理 /health 健康检查"""
        payload = {
            "status": "ok",
            "version": __version__,
            "gateway": self.gateway.gateway_url,
            "models": len(self.model_mapper.available_models),
            "requests": self.stats.total_requests,
            "tokens": self.stats.total_tokens,
            "errors": self.stats.errors,
            "uptime": self.stats.uptime_display,
            "token_expires_in": self.auth.remaining_display,
            "rate_limit": (
                self.rate_limiter.stats() if self.rate_limiter is not None
                else {"enabled": False}
            ),
        }
        if self.gateway_pool is not None:
            payload["gateways"] = self.gateway_pool.status()
        self._respond_json(200, payload)

    def _handle_index(self):
        """处理 / 根路径"""
        html = f"""<!DOCTYPE html>
<html>
<head><title>AtomCode Open API</title></head>
<body style="font-family:system-ui;max-width:600px;margin:40px auto;padding:20px">
<h1>AtomCode Open API</h1>
<p>OpenAI-compatible proxy for AtomCode CodingPlan.</p>
<h2>Endpoints</h2>
<ul>
<li><code>GET  /v1/models</code> - List available models</li>
<li><code>POST /v1/chat/completions</code> - Chat completions</li>
<li><code>GET  /health</code> - Health check & stats</li>
<li><code>GET  /stats</code> - Persistent request stats (?hours=N)</li>
</ul>
<h2>Usage</h2>
<pre>
base_url: http://127.0.0.1:8899/v1
api_key:  any-string (ignored)
</pre>
<h2>Stats</h2>
<ul>
<li>Models: {len(self.model_mapper.available_models)}</li>
<li>Requests: {self.stats.total_requests}</li>
<li>Tokens: {self.stats.total_tokens}</li>
<li>Uptime: {self.stats.uptime_display}</li>
<li>Token expires: {self.auth.remaining_display}</li>
</ul>
</body>
</html>"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(html.encode())

    # ── Helpers ───────────────────────────────────────────────────────

    def _respond_json(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def _log(self, message: str):
        sys.stderr.write(f"  {message}\n")
        sys.stderr.flush()

    def _log_request(self, *, start_ts: float, status_code: int,
                     model_requested: str, model_mapped: str,
                     is_stream: bool,
                     token_count_prompt: Optional[int] = None,
                     token_count_completion: Optional[int] = None,
                     error_message: Optional[str] = None) -> None:
        """把当前请求记录写入异步日志队列（不阻塞）。"""
        if self.log_writer is None:
            return
        try:
            latency_ms = int((time.time() - start_ts) * 1000)
            gateway_used = getattr(self.gateway, "gateway_url", "") if self.gateway else ""
            entry = LogEntry(
                timestamp=int(start_ts),
                method=self.command or "POST",
                path=self.path.split("?", 1)[0],
                model_requested=model_requested or None,
                model_mapped=model_mapped or None,
                status_code=status_code,
                latency_ms=latency_ms,
                is_stream=is_stream,
                token_count_prompt=token_count_prompt,
                token_count_completion=token_count_completion,
                error_message=error_message,
                gateway_used=gateway_used,
            )
            self.log_writer.log(entry)
        except Exception:
            # 日志失败绝不影响业务响应
            pass

    def log_message(self, format, *args):
        # 只记录非健康检查的请求
        msg = format % args
        if "/health" not in msg:
            sys.stderr.write(f"  {msg}\n")
            sys.stderr.flush()


def create_handler_class(gateway: Gateway, model_mapper: ModelMapper,
                         auth: AuthInfo, verbose: bool = False,
                         log_db_path: Optional[str] = None,
                         log_writer: Optional[AsyncLogWriter] = None,
                         rate_limiter: Optional[RateLimiter] = None,
                         gateway_pool: Optional[GatewayPool] = None):
    """
    创建绑定了依赖的请求处理器类

    Args:
        gateway: API 网关客户端
        model_mapper: 模型名称映射器
        auth: 认证信息
        verbose: 是否输出详细日志
        log_db_path: 日志 SQLite 路径；为 None 时使用默认路径
        log_writer: 可选的预构造 AsyncLogWriter（测试时方便注入）
        rate_limiter: 可选的 RateLimiter；为 None 时不启用限流

    Returns:
        (BoundHandler, stats, log_writer)
    """
    stats = RequestStats()

    if log_writer is None:
        db_path = log_db_path or default_db_path()
        log_writer = AsyncLogWriter(db_path)
    else:
        db_path = log_writer.db_path

    class BoundHandler(ProxyHandler):
        pass

    BoundHandler.gateway = gateway
    BoundHandler.model_mapper = model_mapper
    BoundHandler.stats = stats
    BoundHandler.auth = auth
    BoundHandler.log_writer = log_writer
    BoundHandler.log_db_path = db_path
    BoundHandler.rate_limiter = rate_limiter
    BoundHandler.gateway_pool = gateway_pool
    BoundHandler.verbose = verbose

    return BoundHandler, stats, log_writer
