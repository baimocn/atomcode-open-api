"""
gateway - AtomCode API 网关通信

处理与 AtomCode API 网关 (api-ai.gitcode.com) 的所有通信，
包括模型列表获取、请求转发和流式响应处理。
"""

import hashlib
import json
import os
import ssl
import time
import urllib.request
import urllib.error
from typing import Dict, Generator, Optional, Tuple

from .signing import fake_sign_headers

# 默认网关地址
DEFAULT_GATEWAY = "https://api-ai.gitcode.com"

# SSL 上下文（跳过证书验证，与 AtomCode 行为一致）
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


class GatewayError(Exception):
    """网关错误"""
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message
        super().__init__(f"HTTP {status}: {code} - {message}")


class Gateway:
    """AtomCode API 网关客户端"""

    def __init__(self, access_token: str, gateway_url: str = DEFAULT_GATEWAY,
                 user_id: str = ""):
        self.access_token = access_token
        self.gateway_url = gateway_url.rstrip("/")
        self.user_id = user_id
        self._model_cache: Optional[Dict] = None
        self._model_cache_time: float = 0
        self._model_cache_ttl: float = 300  # 5 分钟缓存
        # 最近一次请求的耗时（毫秒）；-1 表示请求异常
        self.latency_ms: int = 0

    def _build_headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """构建请求头"""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        headers.update(fake_sign_headers())
        if extra:
            headers.update(extra)
        return headers

    def _request(self, method: str, path: str, body: Optional[bytes] = None,
                 extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
        """
        发送 HTTP 请求

        Returns:
            (status_code, response_body)
        """
        url = f"{self.gateway_url}{path}"
        headers = self._build_headers(extra_headers)

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        start = time.monotonic()
        try:
            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=120) as resp:
                status, data = resp.status, resp.read()
                self.latency_ms = int((time.monotonic() - start) * 1000)
                return status, data
        except urllib.error.HTTPError as e:
            self.latency_ms = int((time.monotonic() - start) * 1000)
            return e.code, e.read()
        except Exception as e:
            self.latency_ms = -1
            raise GatewayError(0, "NETWORK_ERROR", str(e))

    def fetch_models(self) -> Dict:
        """
        获取可用模型列表

        Returns:
            OpenAI 格式的模型列表 JSON
        """
        now = time.time()
        if self._model_cache and (now - self._model_cache_time) < self._model_cache_ttl:
            return self._model_cache

        status, body = self._request("GET", "/v1/models")
        if status != 200:
            raise GatewayError(status, "MODELS_ERROR", body.decode("utf-8", errors="replace")[:200])

        data = json.loads(body)
        self._model_cache = data
        self._model_cache_time = now
        return data

    def chat_completions(self, body: bytes, stream: bool = False) -> Tuple[int, bytes]:
        """
        发送聊天补全请求

        Args:
            body: JSON 请求体
            stream: 是否流式

        Returns:
            (status_code, response_body)
        """
        try:
            return self._request("POST", "/v1/chat/completions", body)
        except GatewayError:
            self.latency_ms = -1
            raise

    def chat_completions_stream(self, body: bytes) -> Generator[bytes, None, None]:
        """
        发送流式聊天补全请求

        Yields:
            SSE data 行的原始字节
        """
        url = f"{self.gateway_url}/v1/chat/completions"
        headers = self._build_headers()

        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        start = time.monotonic()
        try:
            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=300) as resp:
                buffer = b""
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        line = line.strip()
                        if line:
                            yield line
                if buffer.strip():
                    yield buffer.strip()
            self.latency_ms = int((time.monotonic() - start) * 1000)
        except urllib.error.HTTPError as e:
            self.latency_ms = -1
            error_body = e.read()
            yield f'data: {json.dumps({"error": {"message": error_body.decode("utf-8", errors="replace"), "code": e.code}})}'.encode()
            yield b"data: [DONE]"
        except Exception as e:
            self.latency_ms = -1
            yield f'data: {json.dumps({"error": {"message": str(e)}})}'.encode()
            yield b"data: [DONE]"
