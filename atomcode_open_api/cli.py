#!/usr/bin/env python3
"""
AtomCode Open API - 命令行入口

将 AtomCode CodingPlan 的免费模型额度转为标准 OpenAI API
"""

import argparse
import signal
import socketserver
import sys
import threading

from . import __version__
from .auth import require_auth
from .gateway import Gateway
from .models import ModelMapper
from .server import create_handler_class


def print_banner(gateway_url: str, port: int, model_count: int, auth, stats=None):
    """打印启动信息"""
    print(f"\n{'=' * 60}", file=sys.stderr)
    print(f"  AtomCode Open API v{__version__}", file=sys.stderr)
    print(f"{'=' * 60}", file=sys.stderr)
    print(f"  Local:   http://127.0.0.1:{port}", file=sys.stderr)
    print(f"  Gateway: {gateway_url}", file=sys.stderr)
    print(f"  Models:  {model_count} available", file=sys.stderr)
    print(f"  Token:   expires in {auth.remaining_display}", file=sys.stderr)
    print(f"{'=' * 60}", file=sys.stderr)
    print(f"\n  Use with any OpenAI-compatible client:", file=sys.stderr)
    print(f"    base_url = http://127.0.0.1:{port}/v1", file=sys.stderr)
    print(f"    api_key  = any-string", file=sys.stderr)
    print(f"\n  Popular models:", file=sys.stderr)
    print(f"    deepseek-ai/DeepSeek-V4-Flash  (fast, 1M ctx)", file=sys.stderr)
    print(f"    deepseek-ai/DeepSeek-R1        (reasoning)", file=sys.stderr)
    print(f"    zai-org/GLM-5.1                (Zhipu)", file=sys.stderr)
    print(f"    Qwen/Qwen3-235B-A22B           (large MoE)", file=sys.stderr)
    print(f"    MoonshotAI/Kimi-K2.6           (Moonshot)", file=sys.stderr)
    print(f"\n  Endpoints:", file=sys.stderr)
    print(f"    GET  /v1/models    - List models", file=sys.stderr)
    print(f"    POST /v1/chat/completions - Chat", file=sys.stderr)
    print(f"    GET  /health       - Stats", file=sys.stderr)
    print(f"    GET  /             - Web UI", file=sys.stderr)
    print(f"\n  Press Ctrl+C to stop", file=sys.stderr)
    print(file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        prog="atomcode-open-api",
        description="AtomCode CodingPlan 免费额度 → OpenAI API 反向代理",
    )
    parser.add_argument(
        "-p", "--port", type=int, default=8899,
        help="监听端口 (default: 8899)"
    )
    parser.add_argument(
        "-H", "--host", default="127.0.0.1",
        help="监听地址 (default: 127.0.0.1)"
    )
    parser.add_argument(
        "-g", "--gateway", default="https://api-ai.gitcode.com",
        help="AtomCode API 网关地址"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="输出详细日志"
    )
    parser.add_argument(
        "--version", action="version",
        version=f"atomcode-open-api {__version__}"
    )

    args = parser.parse_args()

    # 加载认证
    auth = require_auth()

    # 初始化网关客户端
    gateway = Gateway(
        access_token=auth.access_token,
        gateway_url=args.gateway,
        user_id=auth.user_id,
    )

    # 初始化模型映射器
    model_mapper = ModelMapper()

    # 预加载模型列表
    print("Loading models...", file=sys.stderr)
    try:
        models_data = gateway.fetch_models()
        model_count = model_mapper.update_from_api(models_data)
    except Exception as e:
        print(f"Warning: Could not fetch models: {e}", file=sys.stderr)
        model_count = 0

    # 创建处理器
    handler_class, stats = create_handler_class(
        gateway=gateway,
        model_mapper=model_mapper,
        auth=auth,
        verbose=args.verbose,
    )

    # 创建服务器（允许端口复用）
    socketserver.TCPServer.allow_reuse_address = True
    try:
        server = socketserver.ThreadingTCPServer((args.host, args.port), handler_class)
    except OSError as e:
        print(f"Error: Cannot bind to {args.host}:{args.port}: {e}", file=sys.stderr)
        print(f"Is another instance already running?", file=sys.stderr)
        sys.exit(1)

    # 优雅关闭
    def shutdown(sig=None, frame=None):
        print("\nShutting down...", file=sys.stderr)
        threading.Thread(target=server.shutdown).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # 打印启动信息
    print_banner(args.gateway, args.port, model_count, auth, stats)

    # 启动服务器
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print(f"\nStopped. Served {stats.total_requests} requests, "
              f"{stats.total_tokens} tokens.", file=sys.stderr)


if __name__ == "__main__":
    main()
