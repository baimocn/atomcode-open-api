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
from typing import Optional

from . import __version__
from .auth import AuthInfo
from .gateway import Gateway, GatewayError
from .models import ModelMapper


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
    verbose: bool = False

    def do_GET(self):
        if self.path == "/v1/models":
            self._handle_models()
        elif self.path == "/health":
            self._handle_health()
        elif self.path == "/":
            self._handle_index()
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
        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            self._respond_json(400, {"error": {"message": "Invalid JSON body", "type": "invalid_request_error"}})
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
            self._handle_stream(body)
        else:
            self._handle_non_stream(body)

    def _handle_non_stream(self, body: bytes):
        """处理非流式请求"""
        try:
            status, resp_body = self.gateway.chat_completions(body)
            if status == 200:
                try:
                    data = json.loads(resp_body)
                    usage = data.get("usage", {})
                    tokens = usage.get("total_tokens", 0)
                    self.stats.record(tokens=tokens)
                except:
                    self.stats.record()
            else:
                self.stats.record(error=True)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(resp_body)
        except GatewayError as e:
            self.stats.record(error=True)
            self._respond_json(e.status or 502, {"error": {"message": e.message, "type": "server_error"}})

    def _handle_stream(self, body: bytes):
        """处理流式请求"""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self._send_cors_headers()
            self.end_headers()

            for line in self.gateway.chat_completions_stream(body):
                self.wfile.write(line + b"\n")
                self.wfile.flush()

            self.stats.record()
        except Exception as e:
            self.stats.record(error=True)
            error_data = json.dumps({"error": {"message": str(e)}})
            self.wfile.write(f"data: {error_data}\n".encode())
            self.wfile.write(b"data: [DONE]\n")
            self.wfile.flush()

    def _handle_models(self):
        """处理 /v1/models 请求"""
        try:
            models_data = self.gateway.fetch_models()
            self.model_mapper.update_from_api(models_data)
            self._respond_json(200, self.model_mapper.get_openai_models_response())
        except GatewayError as e:
            self._respond_json(e.status or 502, {"error": {"message": e.message}})

    def _handle_health(self):
        """处理 /health 健康检查"""
        self._respond_json(200, {
            "status": "ok",
            "version": __version__,
            "gateway": self.gateway.gateway_url,
            "models": len(self.model_mapper.available_models),
            "requests": self.stats.total_requests,
            "tokens": self.stats.total_tokens,
            "errors": self.stats.errors,
            "uptime": self.stats.uptime_display,
            "token_expires_in": self.auth.remaining_display,
        })

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

    def log_message(self, format, *args):
        # 只记录非健康检查的请求
        msg = format % args
        if "/health" not in msg:
            sys.stderr.write(f"  {msg}\n")
            sys.stderr.flush()


def create_handler_class(gateway: Gateway, model_mapper: ModelMapper,
                         auth: AuthInfo, verbose: bool = False):
    """
    创建绑定了依赖的请求处理器类

    Args:
        gateway: API 网关客户端
        model_mapper: 模型名称映射器
        auth: 认证信息
        verbose: 是否输出详细日志

    Returns:
        可用于 HTTPServer 的处理器类
    """
    stats = RequestStats()

    class BoundHandler(ProxyHandler):
        pass

    BoundHandler.gateway = gateway
    BoundHandler.model_mapper = model_mapper
    BoundHandler.stats = stats
    BoundHandler.auth = auth
    BoundHandler.verbose = verbose

    return BoundHandler, stats
