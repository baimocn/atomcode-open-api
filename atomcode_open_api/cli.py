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
from .health_checker import GatewayHealthChecker, GatewayPool
from .logger_db import default_db_path
from .models import ModelMapper
from .rate_limiter import RateLimiter
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
    print(f"    GET  /stats        - Persistent stats (?hours=N)", file=sys.stderr)
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
        "-g", "--gateway", nargs="+",
        default=["https://api-ai.gitcode.com"],
        help="AtomCode API 网关地址（可指定多个以启用故障转移）"
    )
    parser.add_argument(
        "--health-interval", type=float, default=30.0,
        help="多网关健康探测周期，秒 (default: 30.0)"
    )
    parser.add_argument(
        "--health-timeout", type=float, default=10.0,
        help="单次健康探测超时，秒 (default: 10.0)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="输出详细日志"
    )
    parser.add_argument(
        "--log-db", default=None,
        help=f"请求日志 SQLite 路径 (default: {default_db_path()})"
    )
    parser.add_argument(
        "--rate-limit", type=float, default=10.0,
        help="每秒补充令牌数 (default: 10.0；<=0 时禁用速率限制)"
    )
    parser.add_argument(
        "--queue-size", type=int, default=50,
        help="速率限制队列最大长度 (default: 50)"
    )
    parser.add_argument(
        "--queue-timeout", type=float, default=30.0,
        help="队列中请求最长等待秒数 (default: 30.0)"
    )
    parser.add_argument(
        "--version", action="version",
        version=f"atomcode-open-api {__version__}"
    )

    args = parser.parse_args()

    # 加载认证
    auth = require_auth()

    # 解析网关参数（保持向后兼容：args.gateway 既可能是单个字符串也可能是列表）
    gw_list = args.gateway if isinstance(args.gateway, list) else [args.gateway]
    gw_list = [g for g in gw_list if g]
    if not gw_list:
        print("Error: at least one --gateway must be provided", file=sys.stderr)
        sys.exit(1)

    # 主网关：单网关模式直接用它；多网关模式仍把第一个作为 "主" 用于 banner/fallback
    primary_gateway_url = gw_list[0]
    gateway = Gateway(
        access_token=auth.access_token,
        gateway_url=primary_gateway_url,
        user_id=auth.user_id,
    )

    # 多网关：构造 GatewayPool + GatewayHealthChecker
    gateway_pool = None
    health_checker = None
    if len(gw_list) > 1:
        health_checker = GatewayHealthChecker(
            interval=args.health_interval,
            timeout=args.health_timeout,
        )
        gateway_pool = GatewayPool(
            gateway_urls=gw_list,
            access_token=auth.access_token,
            health_checker=health_checker,
        )
        # 启动周期性健康探测
        health_checker.start(gateway_pool.probe_all)
        print(f"  Gateways: {len(gw_list)} (failover enabled)", file=sys.stderr)
        for url in gw_list:
            print(f"    - {url}", file=sys.stderr)

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

    # 速率限制（rate <= 0 时禁用）
    if args.rate_limit and args.rate_limit > 0:
        capacity = max(1, int(round(args.rate_limit)))
        rate_limiter = RateLimiter(
            rate=args.rate_limit,
            capacity=capacity,
            queue_size=args.queue_size,
            queue_timeout=args.queue_timeout,
        )
        print(f"  Rate limit: {args.rate_limit:g} req/s, queue={args.queue_size}, "
              f"timeout={args.queue_timeout:g}s", file=sys.stderr)
    else:
        rate_limiter = None
        print("  Rate limit: disabled", file=sys.stderr)

    # 创建处理器（含异步日志写入器、可选限流器与可选网关池）
    handler_class, stats, log_writer = create_handler_class(
        gateway=gateway,
        model_mapper=model_mapper,
        auth=auth,
        verbose=args.verbose,
        log_db_path=args.log_db,
        rate_limiter=rate_limiter,
        gateway_pool=gateway_pool,
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
    banner_gw = ", ".join(gw_list) if len(gw_list) > 1 else gw_list[0]
    print_banner(banner_gw, args.port, model_count, auth, stats)

    # 启动服务器
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        try:
            log_writer.stop(drain=True, timeout=5.0)
        except Exception:
            pass
        if health_checker is not None:
            try:
                health_checker.stop(timeout=2.0)
            except Exception:
                pass
        print(f"\nStopped. Served {stats.total_requests} requests, "
              f"{stats.total_tokens} tokens.", file=sys.stderr)


if __name__ == "__main__":
    main()
